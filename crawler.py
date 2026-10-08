"""Async web crawler: one frontier, per-host politeness, pluggable fetchers.

uv run python crawler.py --backend local --workers 16 --interval 0
uv run python crawler.py --backend http --sitemap --max-depth 0 \
    --seed https://example.com/ --include '/blog/' --state state.json
"""
import argparse
import asyncio
import email.utils
import heapq
import html
import itertools
import json
import os
import random
import re
import sys
import time
import xml.etree.ElementTree as ET
import zlib
from collections import Counter
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from urllib.parse import urldefrag, urljoin, urlsplit

import aiohttp
import psutil
from playwright.async_api import TimeoutError as PlaywrightTimeout
from playwright.async_api import async_playwright
# Protego, the robots.txt parser of major Python crawlers; 0.6.2 fixed a ReDoS
from protego import Protego
# selectolax 1.0 removed selectolax.parser; import the lexbor backend directly
from selectolax.lexbor import LexborHTMLParser

USER_AGENT = "BookCrawler/1.0 (+https://example.com/crawler-contact)"
AGENT = USER_AGENT.split("/")[0]  # robots.txt groups match the product token
SITEMAP_MAX = 50 * 2**20  # the sitemap protocol's limit, uncompressed
# State field -> request header that asks "has this changed since?"
VALIDATORS = (("last_modified", "If-Modified-Since"),
              ("etag", "If-None-Match"))
SITEMAP_NS = "{http://www.sitemaps.org/schemas/sitemap/0.9}"
# Cloudflare's Content-Signal and IETF AI Preferences' Content-Usage
AI_PREFERENCE_LINES = ("content-signal:", "content-usage:")
PRODUCT_TYPES = {"Product", "ProductGroup", "ProductModel",
                 "IndividualProduct"}
OUTCOMES = ("ok", "unchanged", "gone", "server_error", "blocked",
            "payment_required")


def ld_kind(node) -> int | None:
    """0 for any article type, 1 for a product, None for anything else.

    schema.org has many subtypes: BBC articles say ReportageNewsArticle, and
    Shopify product pages say ProductGroup. Match the family, not one name.
    """
    if not isinstance(node, dict):
        return None
    kinds = node.get("@type")
    for kind in kinds if isinstance(kinds, list) else [kinds]:
        if not isinstance(kind, str):
            continue
        if kind.endswith(("Article", "Posting")) or kind == "Report":
            return 0  # a review page also has a Product node: the article wins
        if kind in PRODUCT_TYPES:
            return 1
    return None
# One URLPattern per extension: "*.{png,jpg,jpeg}" parses but matches nothing
BLOCK = [f"*://*/*.{ext}" for ext in
         "png jpg jpeg gif webp avif svg ico woff woff2 ttf otf mp4 webm"
         .split()]
TEN_SECONDS = aiohttp.ClientTimeout(total=10)
RENDER_WAIT_MS = 15_000  # how long a browser page may take to show its text
THIRTY_SECONDS = aiohttp.ClientTimeout(total=30)
# Markers from challenge pages themselves. Body words such as "captcha" are
# not enough: a page about CAPTCHAs contains them too
CHALLENGE = re.compile(
    r"<title>\s*(just a moment|attention required|access denied)"
    r"|captcha-delivery\.com|id=\"px-captcha", re.I)


def normalize(url: str) -> str:
    url, _ = urldefrag(url)
    p = urlsplit(url)
    return p._replace(netloc=p.netloc.lower(), path=p.path or "/").geturl()


TICKET = itertools.count()


@dataclass(order=True)
class Job:
    priority: int
    url: str = field(compare=False)
    depth: int = field(compare=False, default=0)
    attempt: int = field(compare=False, default=0)
    browser: bool = field(compare=False, default=False)  # retry in a browser
    # Equal priorities leave in arrival order; without this, heapq pops ties
    # in an order that is neither first-in nor last-in
    seq: int = field(default_factory=lambda: next(TICKET))


@dataclass
class Page:
    status: int
    url: str
    html: str
    retry_after: float | None = None
    last_modified: str | None = None
    etag: str | None = None
    challenged: bool = False  # Cloudflare sets cf-mitigated: challenge
    price: str | None = None  # Pay Per Crawl's crawler-price header


class Frontier:
    """One queue per host, plus a schedule of when each host may be asked next.

    get() hands out a job only from a host that is ready now, so a worker never
    sleeps on a job that another host could have served in the meantime.
    """

    def __init__(self, per_host: int, interval: float):
        self.per_host, self.interval = per_host, interval
        self.queues: dict[str, list[Job]] = {}  # a priority heap per host
        self.busy: Counter[str] = Counter()  # requests in flight per host
        self.next_at: dict[str, float] = {}  # earliest next request start
        self.paused_until: dict[str, float] = {}  # blocks and Retry-After
        self.intervals: dict[str, float] = {}  # robots.txt Crawl-delay
        self.ready: list[tuple[float, int, str]] = []  # (when, tiebreak, host)
        self.scheduled: set[str] = set()
        # Browser tabs are a resource too: a browser job leaves only when one
        # is free, so its request starts at the time the schedule allowed
        self.tabs = 0
        self.browser_hosts: set[str] = set()  # hosts that need a browser
        self.tab_waiting: set[str] = set()
        self.unfinished, self.closed = 0, False
        self.wake, self.idle = asyncio.Event(), asyncio.Event()
        self.idle.set()

    def qsize(self) -> int:
        return sum(len(q) for q in self.queues.values())

    def slow_to(self, host: str, seconds: float) -> None:
        old = self.intervals.get(host, self.interval)
        new = self.intervals[host] = max(self.interval, seconds)
        if host in self.next_at:  # the next request is already timed: move it
            self.next_at[host] += new - old

    def pause(self, host: str, seconds: float) -> None:
        until = time.monotonic() + seconds
        self.paused_until[host] = max(self.paused_until.get(host, 0), until)

    def _schedule(self, host: str) -> None:
        if (host in self.scheduled or not self.queues.get(host)
                or self.busy[host] >= self.per_host):
            return
        when = max(self.next_at.get(host, 0), self.paused_until.get(host, 0))
        heapq.heappush(self.ready, (when, next(TICKET), host))
        self.scheduled.add(host)
        self.wake.set()

    def put(self, job: Job, delay: float = 0.0) -> None:
        self.unfinished += 1  # counted now, so join() waits for a delayed job
        self.idle.clear()
        if delay > 0:  # a backoff waits here, not inside a worker
            asyncio.get_running_loop().call_later(delay, self._push, job)
        else:
            self._push(job)

    def _push(self, job: Job) -> None:
        host = urlsplit(job.url).netloc
        heapq.heappush(self.queues.setdefault(host, []), job)
        self._schedule(host)

    async def get(self) -> Job:
        while not self.closed:
            now = time.monotonic()
            if self.ready and self.ready[0][0] <= now:
                _, _, host = heapq.heappop(self.ready)
                self.scheduled.discard(host)
                if max(self.next_at.get(host, 0),
                       self.paused_until.get(host, 0)) > now:
                    self._schedule(host)  # paused or slowed since scheduled
                    continue
                queue = self.queues[host]
                if host in self.browser_hosts:
                    queue[0].browser = True  # learned after it was queued
                if queue[0].browser:
                    if self.tabs == 0:
                        self.tab_waiting.add(host)  # waits for a tab
                        continue
                    self.tabs -= 1
                job = heapq.heappop(queue)
                self.busy[host] += 1
                self.next_at[host] = now + self.intervals.get(host,
                                                              self.interval)
                self._schedule(host)  # its next job, if under its limit
                return job
            self.wake.clear()
            wait = self.ready[0][0] - now if self.ready else None
            try:
                await asyncio.wait_for(self.wake.wait(), wait)
            except TimeoutError:
                pass
        raise asyncio.QueueShutDown

    def task_done(self, job: Job) -> None:
        host = urlsplit(job.url).netloc
        self.busy[host] -= 1
        if job.browser:
            self.tabs += 1
            waiting, self.tab_waiting = self.tab_waiting, set()
            for other in waiting:
                self._schedule(other)
        self._schedule(host)
        self.unfinished -= 1
        if self.unfinished == 0:
            self.idle.set()

    async def join(self) -> None:
        await self.idle.wait()

    def shutdown(self) -> None:
        self.closed = True
        self.wake.set()


def site_root(url: str) -> str:
    return "{0.scheme}://{0.netloc}".format(urlsplit(url))


DISALLOW_ALL = "User-agent: *\nDisallow: /"


class Robots:
    def __init__(self, http: aiohttp.ClientSession, agent: str = AGENT):
        self.http, self.cache, self.signals = http, {}, {}
        self.agent = agent
        self.fetched: dict[str, float] = {}  # RFC 9309: refetch after 24 h

    async def allowed(self, url: str) -> bool:
        root = site_root(url)
        if (root not in self.fetched
                or time.monotonic() - self.fetched[root] >= 86_400):
            try:
                async with self.http.get(root + "/robots.txt",
                                         timeout=TEN_SECONDS) as r:
                    status, text = r.status, await r.text(errors="replace")
                if 200 <= status < 300:
                    # Parsers skip AI preference lines: keep them as written
                    self.signals[root] = [
                        line.strip() for line in text.splitlines()
                        if line.lower().startswith(AI_PREFERENCE_LINES)]
                elif 400 <= status < 500:
                    text = ""  # unavailable (4xx): RFC 9309 allows all
                else:
                    text = DISALLOW_ALL  # unreachable (5xx): disallow all
            except (aiohttp.ClientError, TimeoutError):
                text = DISALLOW_ALL  # network error: also unreachable
            self.cache[root] = Protego.parse(text)
            self.fetched[root] = time.monotonic()
        return self.cache[root].can_fetch(url, self.agent)

    def crawl_delay(self, url: str) -> float | None:
        return self.cache[site_root(url)].crawl_delay(self.agent)


async def sitemap_urls(http, robots: Robots, root: str) -> dict[str, str]:
    """Every <loc> reachable from robots.txt Sitemap lines, with lastmod."""
    await robots.allowed(root + "/")  # fetches and caches robots.txt
    todo = list(robots.cache[root].sitemaps) or [root + "/sitemap.xml"]
    found, done = {}, set()
    while todo:
        url = todo.pop()
        if url in done:
            continue
        done.add(url)
        async with http.get(url, timeout=THIRTY_SECONDS) as r:
            if r.status != 200:
                continue
            body = bytearray()  # read(n) returns what has arrived, not n
            async for chunk in r.content.iter_chunked(2**16):
                body += chunk
                if len(body) > SITEMAP_MAX:
                    break
        if body[:2] == b"\x1f\x8b":  # sitemap.xml.gz served as a file
            # Cap the output: a few KB of gzip can expand to gigabytes
            body = zlib.decompressobj(wbits=31).decompress(bytes(body),
                                                           SITEMAP_MAX + 1)
        if len(body) > SITEMAP_MAX:
            continue  # over the protocol limit: skip, do not parse
        try:
            tree = ET.fromstring(body)
        except ET.ParseError:
            continue  # one broken sitemap must not stop the crawl
        for entry in tree:
            loc = entry.findtext(SITEMAP_NS + "loc", "").strip()
            if entry.tag == SITEMAP_NS + "sitemap":
                todo.append(loc)  # a sitemap index points to more sitemaps
            elif loc:
                found[loc] = entry.findtext(SITEMAP_NS + "lastmod", "")
    return found


def classify(page: Page, record: dict | None) -> str:
    """A 200 is not proof of success: check what the page actually is."""
    status = page.status
    if page.challenged:
        return "blocked"
    if status == 304:
        return "unchanged"  # the conditional request found no change
    if status == 402:
        return "payment_required"  # a price, not a block
    if status in (401, 403, 429):
        return "blocked"
    if 400 <= status < 500:
        return "gone"  # 404, 410 and other client errors: do not retry
    if status == 0 or status >= 500:
        return "server_error"
    if record is None and CHALLENGE.search(page.html[:20_000]):
        return "blocked"  # a challenge page served with a 200
    return "ok"


def first(value):
    return value[0] if isinstance(value, list) and value else value


def clean(value):
    # Some CMS plugins HTML-escape strings inside JSON-LD ("&amp;")
    return html.unescape(value) if isinstance(value, str) else value


def structured(tree, url: str) -> dict | None:
    """schema.org JSON-LD first. One page's facts can span several nodes."""
    nodes = []
    for script in tree.css('script[type="application/ld+json"]'):
        try:
            data = json.loads(script.text())
        except ValueError:
            continue
        for item in data if isinstance(data, list) else [data]:
            if isinstance(item, dict):
                nodes += item.get("@graph", [item])
    by_id = {n["@id"]: n for n in nodes if isinstance(n, dict) and "@id" in n}
    main = min((n for n in nodes if ld_kind(n) is not None),
               key=ld_kind, default=None)
    if main is None:
        return None
    # Dates often sit on the WebPage node, not on the Article itself
    dated = main if main.get("datePublished") else next(
        (n for n in nodes if isinstance(n, dict) and n.get("datePublished")),
        main)
    author = first(main.get("author")) or {}
    if isinstance(author, str):
        author = {"name": author}
    author = by_id.get(author.get("@id"), author)  # {"@id": ...} -> Person
    # A ProductGroup prices its variants, not itself
    variant = first(main.get("hasVariant")) or {}
    offer = first(main.get("offers") or variant.get("offers")) or {}
    return {"url": url, "type": first(main.get("@type")),
            "name": clean(main.get("headline") or main.get("name")
                          or dated.get("name")),
            "author": clean(author.get("name")),
            "published": dated.get("datePublished"),
            "modified": dated.get("dateModified"),
            "price": offer.get("price"),
            "currency": offer.get("priceCurrency")}


def join(base: str, href: str) -> str | None:
    try:
        return urljoin(base, href)
    except ValueError:  # e.g. "http://[::1": skip one link, keep the rest
        return None


def parse(url: str, html: str) -> tuple[dict | None, list[str]]:
    tree = LexborHTMLParser(html)
    links = [link for a in tree.css("a[href]")
             if (link := join(url, a.attributes.get("href") or ""))]
    record = structured(tree, url)
    if record:
        return record, links
    quotes = tree.css("div.quote")  # fallback: CSS selectors per site
    if quotes:
        return {"url": url, "quotes": [
            {"text": q.css_first("span.text").text(strip=True),
             "author": q.css_first("small.author").text(strip=True)}
            for q in quotes]}, links
    title = tree.css_first("div.product_main h1")
    if title is None:
        return None, links  # a listing page: follow its links only
    rows = {r.css_first("th").text(): r.css_first("td").text()
            for r in tree.css("table.table tr")}
    return {
        "url": url,
        "title": title.text(strip=True),
        "price": tree.css_first("p.price_color").text(strip=True),
        "availability": tree.css_first("p.availability").text(strip=True),
        "upc": rows.get("UPC"),
    }, links


def page_record(url: str, html: str) -> dict:
    """--pages: a basic record for a page with no structured data or rule."""
    tree = LexborHTMLParser(html)

    def first_of(selector: str, attr: str | None = None) -> str | None:
        node = tree.css_first(selector)
        if node is None:
            return None
        value = node.attributes.get(attr) if attr else node.text(strip=True)
        return clean(value) or None
    return {"url": url, "type": "page", "title": first_of("title"),
            "h1": first_of("h1"),
            "description": first_of('meta[name="description"]', "content"),
            "canonical": first_of('link[rel="canonical"]', "href")}


def to_markdown(url: str, html: str) -> str | None:
    """--markdown: the page's main content, ready for an LLM or a RAG index."""
    import trafilatura  # loaded in the worker processes only
    return trafilatura.extract(html, url=url, output_format="markdown",
                               favor_precision=True)


def looks_empty(html: str) -> bool:
    """An empty app shell: almost no visible text until JavaScript runs."""
    tree = LexborHTMLParser(html)
    for node in tree.css("script, style, noscript, template"):
        node.decompose()
    text = tree.body.text(separator=" ", strip=True) if tree.body else ""
    return len(text) < 500


def retry_after(value: str | None) -> float | None:
    """Seconds or an HTTP date, capped so one header cannot park a worker."""
    if not value:
        return None
    if value.isdigit():
        seconds = float(value)
    else:
        try:
            when = email.utils.parsedate_to_datetime(value)
        except (TypeError, ValueError):
            return None
        seconds = when.timestamp() - time.time()
    return min(max(seconds, 0.0), 300.0)


class HttpFetcher:
    def __init__(self, http: aiohttp.ClientSession):
        self.http = http

    async def slots(self, n: int) -> list["HttpFetcher"]:
        return [self] * n

    async def fetch(self, url: str, validators: dict | None = None) -> Page:
        async with self.http.get(url, timeout=THIRTY_SECONDS,
                                 headers=validators or {}) as r:
            html = await r.text(errors="replace")
            return Page(r.status, str(r.url), html,
                        retry_after(r.headers.get("retry-after")),
                        r.headers.get("last-modified"),
                        r.headers.get("etag"),
                        r.headers.get("cf-mitigated") == "challenge",
                        r.headers.get("crawler-price"))


class BrowserSlot:
    """One tab, owned by one worker, so navigations never race."""

    def __init__(self, page):
        self.page = page

    async def prepare(self) -> None:
        cdp = await self.page.context.new_cdp_session(self.page)
        await cdp.send("Network.enable")
        # Blocked inside the browser: no round trip to Python per request
        await cdp.send("Network.setBlockedURLs", {"urlPatterns": [
            {"urlPattern": p, "block": True} for p in BLOCK]})
        # Revalidate instead of trusting a long-lived disk cache (cheap 304s)
        await self.page.set_extra_http_headers({"Cache-Control": "max-age=0"})

    async def fetch(self, url: str, validators: dict | None = None) -> Page:
        resp = await self.page.goto(url, wait_until="domcontentloaded",
                                    timeout=45_000)
        html = await self.page.content()  # one round trip, parsed locally
        if looks_empty(html):  # an app that loads its data after the HTML
            try:
                await self.page.wait_for_function(
                    "document.body && document.body.innerText.length > 500",
                    timeout=RENDER_WAIT_MS)
            except PlaywrightTimeout:
                pass  # still empty: handle() counts it as empty_after_render
            html = await self.page.content()
        if resp is None:
            return Page(0, self.page.url, html)
        # .headers needs no extra call; header_value() asks the driver again
        return Page(resp.status, self.page.url, html,
                    retry_after(resp.headers.get("retry-after")),
                    resp.headers.get("last-modified"),
                    resp.headers.get("etag"),
                    resp.headers.get("cf-mitigated") == "challenge",
                    resp.headers.get("crawler-price"))


class LocalBrowser:
    def __init__(self, pw):
        self.pw, self.browser = pw, None

    async def slots(self, n: int) -> list[BrowserSlot]:
        self.browser = await self.pw.chromium.launch(headless=True)
        ctx = await self.browser.new_context()
        slots = [BrowserSlot(await ctx.new_page()) for _ in range(n)]
        for s in slots:
            await s.prepare()
        return slots

    async def close(self) -> None:
        if self.browser:  # None when Chromium failed to start
            try:
                await self.browser.close()
            except Exception:  # the driver may already be gone (Ctrl+C)
                pass


class Crawler:
    def __init__(self, seeds, fetcher, http, out, *, max_pages, max_depth,
                 per_host, interval, state=None, browsers=None, include="",
                 state_path=None, pages=False, agent=AGENT, markdown=None,
                 recheck=False):
        self.seeds, self.fetcher, self.out = seeds, fetcher, out
        self.http = http
        # Escalation: a small pool of browser tabs for pages HTTP cannot read
        self.browsers: asyncio.Queue | None = browsers
        self.rendered: Counter[str] = Counter()  # escalations that paid off
        # url -> {"lastmod": from the sitemap,
        #         "last_modified" and "etag": from the server}
        self.state: dict[str, dict] = state if state is not None else {}
        self.state_path = state_path
        self.recheck = recheck  # conditional GET even for unchanged lastmod
        self.lastmod: dict[str, str] = {}  # this run's sitemap dates
        self.stored: set[str] = set()  # final URLs, after redirects
        self.max_pages, self.max_depth = max_pages, max_depth
        self.hosts = {urlsplit(normalize(s)).netloc for s in seeds}  # scope
        self.include = re.compile(include)  # and the URLs found within it
        self.frontier = Frontier(per_host, interval)
        self.seen: set[str] = set()
        self.robots = Robots(http, agent)
        self.pages = pages  # write a basic record for every other page
        self.markdown: ProcessPoolExecutor | None = markdown
        self.prices: dict[str, str] = {}  # url -> crawler-price, from 402s
        self.stats, self.active = Counter(), 0
        self.started = time.monotonic()

    def enqueue(self, url: str, depth: int, attempt: int = 0, *,
                delay: float = 0.0, browser: bool = False) -> None:
        url = normalize(url)
        host = urlsplit(url).netloc
        if attempt == 0 and not browser:  # retries skip the dedupe check
            if (url in self.seen or host not in self.hosts
                    or depth and not self.include.search(url)):
                return
            if len(self.seen) >= self.max_pages:
                return
            self.seen.add(url)
        browser = self.browsers is not None and browser
        # Product pages first, so a capped crawl spends its budget on records.
        # An escalation goes next too: its render shows what the host needs
        is_product = "/catalogue/" in url and "/category/" not in url
        priority = 2 if attempt else (0 if is_product or browser else 1)
        self.frontier.put(Job(priority, url, depth, attempt, browser), delay)

    async def fetch(self, slot, job: Job, validators: dict) -> Page:
        if not job.browser:
            return await slot.fetch(job.url, validators)
        tab = await self.browsers.get()  # borrow a tab, then give it back
        try:
            self.stats["via_browser"] += 1
            return await tab.fetch(job.url)
        finally:
            self.browsers.put_nowait(tab)

    async def handle(self, slot, job: Job) -> None:
        host = urlsplit(job.url).netloc
        if not await self.robots.allowed(job.url):
            self.stats["robots_disallowed"] += 1
            return
        if delay := self.robots.crawl_delay(job.url):
            self.frontier.slow_to(host, delay)
        self.active += 1
        known = self.state.setdefault(job.url, {})
        validators = {h: known[k] for k, h in VALIDATORS if k in known}
        try:
            page = await self.fetch(slot, job, validators)
        except Exception as exc:  # timeouts, closed connections, DNS errors
            page = Page(0, job.url, "")
            self.stats["exception:" + type(exc).__name__] += 1
        finally:
            self.active -= 1

        try:
            record, links = (parse(page.url, page.html) if page.html
                             else (None, []))
        except Exception:  # a page missing one field must not kill a worker
            record, links = None, []
            self.stats["parse_error"] += 1
        outcome = classify(page, record)
        if (self.browsers is not None and not job.browser and outcome == "ok"
                and record is None and looks_empty(page.html)):
            self.stats["escalated"] += 1  # an app shell: render it
            self.enqueue(job.url, job.depth, job.attempt, browser=True)
            return
        if job.browser and outcome == "ok":
            if record is None and looks_empty(page.html):
                self.stats["empty_after_render"] += 1  # watch this number
            else:
                self.rendered[host] += 1
                if self.rendered[host] >= 3:  # stop paying for two fetches
                    self.frontier.browser_hosts.add(host)
        if (record is None and self.pages and outcome == "ok"
                and not looks_empty(page.html)):
            record = page_record(normalize(page.url), page.html)
        self.stats[outcome] += 1
        if outcome in ("ok", "unchanged"):  # commit only what was fetched
            if page.last_modified:
                known["last_modified"] = page.last_modified
            if page.etag:
                known["etag"] = page.etag
            if job.url in self.lastmod:
                known["lastmod"] = self.lastmod[job.url]
            final = normalize(page.url)
            self.seen.add(final)
            if record and final in self.stored:
                self.stats["duplicate"] += 1  # two URLs redirect to one page
            elif record:
                self.stored.add(final)
                record["content_signal"] = self.robots.signals.get(
                    site_root(job.url), [])
                if self.markdown:  # CPU work: keep it off the event loop
                    record["markdown"] = await asyncio.get_running_loop(
                    ).run_in_executor(self.markdown, to_markdown, final,
                                      page.html)
                self.out.write(json.dumps(record) + "\n")
                self.stats["records"] += 1
            if job.depth < self.max_depth:
                for link in links:
                    self.enqueue(link, job.depth + 1)
        elif outcome == "payment_required":
            self.prices[job.url] = page.price or "unknown"  # never escalate
        elif outcome in ("blocked", "server_error") and job.attempt < 3:
            backoff = min(60, 2 ** job.attempt) * random.uniform(0.5, 1.5)
            delay = page.retry_after or backoff
            if outcome == "blocked" or page.retry_after:
                self.frontier.pause(host, delay)  # slow the whole host down
            # A blocked HTTP fetch is retried in a browser, if there is one
            self.enqueue(job.url, job.depth, job.attempt + 1, delay=delay,
                         browser=job.browser or outcome == "blocked")

    async def worker(self, slot) -> None:
        while True:
            try:
                job = await self.frontier.get()
            except asyncio.QueueShutDown:
                return
            try:
                await self.handle(slot, job)
            finally:
                self.frontier.task_done(job)

    def checkpoint(self) -> None:
        """Save recrawl state atomically, after the records it describes."""
        if not self.state_path:
            return
        self.out.flush()  # a URL marked fetched must have its record on disk
        tmp = self.state_path + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.state, f)
        os.replace(tmp, self.state_path)  # a crash leaves old or new, whole

    async def monitor(self, every: float = 5.0) -> None:
        me = psutil.Process()
        for tick in itertools.count(1):
            await asyncio.sleep(every)
            if tick % 6 == 0:
                self.checkpoint()  # every 30 s, so a crash loses little
            procs = [me, *me.children(recursive=True)]
            rss = sum(p.memory_info().rss for p in procs) / 2**20
            done = sum(self.stats[k] for k in OUTCOMES)
            elapsed = time.monotonic() - self.started
            print(f"[{elapsed:6.1f}s] active={self.active:<3} "
                  f"queued={self.frontier.qsize():<5} done={done:<5} "
                  f"records={self.stats['records']:<5} "
                  f"{done / elapsed:5.2f} pages/s  "
                  f"RSS={rss:,.0f} MB in {len(procs)} procs", flush=True)

    async def from_sitemaps(self) -> None:
        listed = {}
        for seed in self.seeds:
            listed |= await sitemap_urls(self.http, self.robots,
                                         site_root(seed))
        wanted = {u: m for u, m in listed.items() if self.include.search(u)}
        self.stats["sitemap_urls"] = len(wanted)
        for url, lastmod in sorted(wanted.items(), key=lambda kv: kv[1],
                                   reverse=True):  # newest first
            url = normalize(url)
            if (lastmod and not self.recheck
                    and self.state.get(url, {}).get("lastmod") == lastmod):
                self.stats["skipped_unchanged"] += 1  # zero requests
                continue
            self.lastmod[url] = lastmod
            self.enqueue(url, 0)

    async def run(self, workers: int, sitemap: bool = False) -> float:
        slots = await self.fetcher.slots(workers)
        if self.browsers is not None:
            self.frontier.tabs = self.browsers.qsize()
            if not self.frontier.tabs:
                raise RuntimeError("--escalate opened no browser tabs")
        if sitemap:
            await self.from_sitemaps()
        else:
            for seed in self.seeds:
                self.enqueue(seed, 0)
        self.started = time.monotonic()
        monitor = asyncio.create_task(self.monitor())
        tasks = [asyncio.create_task(self.worker(s), name=f"worker-{i}")
                 for i, s in enumerate(slots)]
        try:
            await self.frontier.join()  # every queued job was handled
        except asyncio.CancelledError:  # Ctrl+C: stop work before cleanup
            for task in (*tasks, monitor):
                task.cancel()
            raise
        self.frontier.shutdown()  # idle workers get QueueShutDown and exit
        await asyncio.gather(*tasks)
        monitor.cancel()
        return time.monotonic() - self.started


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--seed", nargs="+", default=["https://books.toscrape.com/"],
                    help="one or more start URLs; their hosts are the scope")
    ap.add_argument("--backend", choices=["http", "local", "gologin"],
                    default="local")
    ap.add_argument("--workers", type=int, default=8)
    ap.add_argument("--profiles", default="", help="comma-separated IDs")
    ap.add_argument("--tabs", type=int, default=4, help="tabs per profile")
    ap.add_argument("--escalate", choices=["local", "gologin"],
                    help="http backend: retry app shells and blocked pages "
                         "in a browser")
    ap.add_argument("--max-pages", type=int, default=1000)
    ap.add_argument("--max-depth", type=int, default=5)
    ap.add_argument("--per-host", type=int, default=8)
    ap.add_argument("--interval", type=float, default=1.0,
                    help="seconds between requests to one host")
    ap.add_argument("--out", default="books.jsonl")
    ap.add_argument("--pages", action="store_true",
                    help="also write a basic record for pages without one")
    ap.add_argument("--markdown", action="store_true",
                    help="add each page's main content as markdown")
    ap.add_argument("--user-agent", default=USER_AGENT,
                    help="name and contact URL that sites see")
    ap.add_argument("--sitemap", action="store_true",
                    help="seed the frontier from robots.txt sitemaps")
    ap.add_argument("--include", default="",
                    help="regex: crawl only URLs that match")
    ap.add_argument("--state", help="JSON file for incremental recrawls")
    ap.add_argument("--recheck", action="store_true",
                    help="with --state: send a conditional GET for every "
                         "URL, even when its lastmod hasn't changed")
    args = ap.parse_args()
    if "gologin" in (args.backend, args.escalate):
        if not os.environ.get("GL_API_TOKEN"):
            ap.error("set GL_API_TOKEN to your Gologin API token "
                     "(dashboard > API & MCP > API)")
        if not args.profiles:
            ap.error("pass --profiles; create them once with "
                     "`uv run python gologin_pool.py 3 > profiles.txt`")
    state = {}
    if args.state:
        try:
            with open(args.state) as f:
                state = json.load(f)
        except FileNotFoundError:
            pass

    headers = {"User-Agent": args.user_agent}
    # aiohttp opens at most 100 connections by default; size the pool to the
    # workers so a large crawl is not capped silently
    connector = aiohttp.TCPConnector(limit=max(100, args.workers))
    async with aiohttp.ClientSession(headers=headers,
                                     connector=connector) as http, \
            async_playwright() as pw:
        def browser_fetcher(kind: str):
            if kind == "local":
                return LocalBrowser(pw)
            from gologin_pool import GologinPool
            return GologinPool(pw, http, args.profiles.split(","), args.tabs)

        fetcher, escalation, browsers = None, None, None
        if args.backend == "http":
            fetcher = HttpFetcher(http)
            if args.escalate:
                escalation = browser_fetcher(args.escalate)
                browsers = asyncio.Queue()
        else:
            fetcher = browser_fetcher(args.backend)
            if args.backend == "gologin":
                args.workers = len(args.profiles.split(",")) * args.tabs
        # Markdown costs milliseconds of CPU per page. Threads do not help:
        # lxml turns the GIL back on in free-threaded builds, and it does
        # not load in subinterpreters, so the work goes to processes
        markdown = ProcessPoolExecutor() if args.markdown else None
        # A recrawl appends: a page that did not change keeps its old line
        with open(args.out, "a" if args.state else "w") as out:
            crawler = Crawler(args.seed, fetcher, http, out,
                              max_pages=args.max_pages,
                              max_depth=args.max_depth,
                              per_host=args.per_host, interval=args.interval,
                              state=state, browsers=browsers,
                              include=args.include, state_path=args.state,
                              pages=args.pages, markdown=markdown,
                              recheck=args.recheck,
                              agent=args.user_agent.split("/")[0])
            print(f"Starting the {args.backend} crawl. Progress prints every "
                  "5 seconds; press Ctrl+C to stop.", file=sys.stderr,
                  flush=True)
            try:
                if escalation:  # inside try: a half-open pool still closes
                    for tab in await escalation.slots(args.tabs):
                        browsers.put_nowait(tab)
                elapsed = await crawler.run(args.workers, args.sitemap)
            finally:
                crawler.checkpoint()  # also after a crash: keep what was done
                if markdown:
                    markdown.shutdown()
                for f in (fetcher, escalation):
                    if hasattr(f, "close"):
                        await f.close()
    done = sum(crawler.stats[k] for k in OUTCOMES)
    crawler.stats["records"] += 0  # print "records": 0 rather than omit it
    print(json.dumps({"backend": args.backend, "workers": args.workers,
                      "seconds": round(elapsed, 1),
                      "pages_per_s": round(done / elapsed, 2),
                      **crawler.stats}))
    if crawler.stats["ok"] and not crawler.stats["records"]:
        print("No records: these pages have no JSON-LD article or product, "
              "and no site-specific selectors in parse() match them. Add "
              "--pages for a url/title record per page, or add selectors.",
              file=sys.stderr)
    if crawler.prices:
        prices = sorted(set(crawler.prices.values()))
        print(f"{len(crawler.prices)} pages answered 402 Payment Required "
              f"(crawler-price: {', '.join(prices[:5])}). Paying needs a "
              "crawler that the site's CDN has verified.", file=sys.stderr)


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\nStopped. Records and state written so far are saved.",
              file=sys.stderr)
        sys.exit(130)  # the usual exit code after Ctrl+C
    except RuntimeError as exc:
        from gologin_pool import CloudSetupError
        if not isinstance(exc, CloudSetupError):
            raise
        sys.exit(f"crawler.py: {exc}")
