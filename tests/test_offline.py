"""Offline tests: run anywhere, with network, browser, and token mocked."""
import asyncio
import email.utils
import gzip
import io
import json
import time

from aiohttp import ClientSession, web
from selectolax.lexbor import LexborHTMLParser

import crawler
from crawler import Frontier, Job


def test_frontier_serves_a_ready_host_first():
    async def run():
        f = Frontier(per_host=1, interval=1.0)
        for i in range(5):
            f.put(Job(1, f"https://slow.test/{i}"))
        f.put(Job(1, "https://other.test/a"))
        first = await f.get()   # slow.test, now busy for 1 second
        t = time.monotonic()
        second = await f.get()  # must not wait behind slow.test
        return first.url, second.url, time.monotonic() - t
    first, second, waited = asyncio.run(run())
    assert first.startswith("https://slow.test/")
    assert second == "https://other.test/a" and waited < 0.1


def test_frontier_spaces_requests_to_one_host():
    async def run():
        f = Frontier(per_host=1, interval=0.3)
        f.put(Job(1, "https://a.test/1"))
        f.put(Job(1, "https://a.test/2"))
        j = await f.get()
        f.task_done(j)
        t = time.monotonic()
        await f.get()
        return time.monotonic() - t
    assert 0.25 < asyncio.run(run()) < 0.6


def test_join_waits_for_a_delayed_retry_and_pause_holds_the_host():
    async def run():
        f = Frontier(per_host=1, interval=0)
        f.pause("a.test", 0.3)
        f.put(Job(2, "https://a.test/retry"), delay=0.1)
        t = time.monotonic()
        job = await f.get()
        waited = time.monotonic() - t
        f.task_done(job)
        await asyncio.wait_for(f.join(), 1)
        return waited
    assert asyncio.run(run()) >= 0.28


def test_looks_empty_flags_a_javascript_shell_only():
    shell = ("<html><body><div id=root></div><script>" + "x" * 5000
             + "</script></body></html>")
    page = "<html><body><p>" + "word " * 200 + "</p></body></html>"
    assert crawler.looks_empty(shell) and not crawler.looks_empty(page)


def test_structured_reads_the_whole_graph():
    graph = {"@graph": [
        {"@type": "Product", "name": "Post title"},
        {"@type": "ItemPage", "datePublished": "2026-01-01"},
        {"@type": "Person", "@id": "#p", "name": "A &amp; B"},
        {"@type": "BlogPosting", "headline": "T &amp; U",
         "author": {"@id": "#p"}},
    ]}
    tree = LexborHTMLParser(
        f'<script type="application/ld+json">{json.dumps(graph)}</script>')
    rec = crawler.structured(tree, "u")
    assert rec["type"] == "BlogPosting" and rec["name"] == "T & U"
    assert rec["author"] == "A & B" and rec["published"] == "2026-01-01"


def test_quotes_fallback_and_malformed_link():
    html = ('<div class="quote"><span class="text">Hi</span>'
            '<small class="author">Ann</small></div>'
            '<a href="/ok">x</a><a href="http://[::1">bad</a>')
    rec, links = crawler.parse("https://q.test/", html)
    assert rec["quotes"] == [{"text": "Hi", "author": "Ann"}]
    assert links == ["https://q.test/ok"]


def test_retry_after_forms_and_cap():
    later = email.utils.formatdate(time.time() + 30, usegmt=True)
    assert crawler.retry_after("120") == 120
    assert crawler.retry_after("86400") == 300
    assert 25 < crawler.retry_after(later) <= 30
    assert crawler.retry_after("soon") is None


def test_robots_rules_status_codes_and_sitemap_bomb():
    bomb = gzip.compress(b"<" * (60 * 2**20))  # 60 MB unpacked

    async def run():
        app = web.Application()
        state = {"robots": 200}

        async def robots(_):
            if state["robots"] != 200:
                return web.Response(status=state["robots"])
            return web.Response(text=(
                "User-agent: *\nCrawl-delay: 2.5\nDisallow: *utm=\n"
                "Content-Signal: ai-train=no\nContent-Usage: train-ai=n\n"
                "Sitemap: http://127.0.0.1:8765/s.xml.gz\n"))

        async def sitemap(_):
            return web.Response(body=bomb)
        app.router.add_get("/robots.txt", robots)
        app.router.add_get("/s.xml.gz", sitemap)
        runner = web.AppRunner(app)
        await runner.setup()
        await web.TCPSite(runner, "127.0.0.1", 8765).start()
        root = "http://127.0.0.1:8765"
        try:
            async with ClientSession() as http:
                r = crawler.Robots(http)
                utm = await r.allowed(root + "/blog/a?utm=1")
                plain = await r.allowed(root + "/blog/a")
                found = await crawler.sitemap_urls(http, r, root)
                out = [utm, plain, r.crawl_delay(root + "/"),
                       r.signals[root], found]
                for code in (404, 503):
                    state["robots"] = code
                    fresh = crawler.Robots(http)
                    out.append(await fresh.allowed(root + "/x"))
                state["robots"] = 200
                stale = crawler.Robots(http)
                await stale.allowed(root + "/x")
                stale.cache[root] = crawler.Protego.parse(crawler.DISALLOW_ALL)
                stale.fetched[root] -= 25 * 3600  # older than 24 hours
                out.append(await stale.allowed(root + "/x"))  # refetched
                return out
        finally:
            await runner.cleanup()
    utm, plain, delay, signals, found, on_404, on_503, refetched = asyncio.run(
        run())
    assert refetched
    assert not utm and plain and delay == 2.5
    assert signals == ["Content-Signal: ai-train=no",
                       "Content-Usage: train-ai=n"] and found == {}
    assert on_404 and not on_503


def test_browser_job_waits_for_a_tab_without_bursting():
    async def run():
        f = Frontier(per_host=8, interval=0.1)
        f.tabs = 1
        for i in range(3):
            f.put(Job(1, f"https://a.test/{i}", browser=True))
        f.put(Job(1, "https://c.test/x"))
        t0 = time.monotonic()
        first = await f.get()               # a.test takes the only tab
        other = await f.get()               # c.test must not wait for a tab
        other_at = time.monotonic() - t0
        starts = [0.0]
        for _ in range(2):
            await asyncio.sleep(0.3)        # the tab is busy for 0.3 s
            f.task_done(first)
            first = await f.get()
            starts.append(time.monotonic() - t0)
        return first.url, other.url, other_at, starts
    _, other, other_at, starts = asyncio.run(run())
    assert other == "https://c.test/x" and other_at < 0.05
    gaps = [b - a for a, b in zip(starts, starts[1:])]
    assert all(g >= 0.1 for g in gaps)      # no burst when the tab frees


def test_learned_host_sends_already_queued_jobs_to_the_browser():
    async def run():
        f = Frontier(per_host=1, interval=0)
        f.tabs = 1
        f.put(Job(1, "https://a.test/1"))
        f.browser_hosts.add("a.test")
        return await f.get()
    assert asyncio.run(run()).browser


def test_crawl_delay_moves_the_next_request():
    async def run():
        f = Frontier(per_host=1, interval=0.1)
        f.put(Job(1, "https://a.test/1"))
        f.put(Job(1, "https://a.test/2"))
        job = await f.get()
        f.slow_to("a.test", 0.5)
        f.task_done(job)
        t = time.monotonic()
        await f.get()
        return time.monotonic() - t
    assert asyncio.run(run()) >= 0.45


def test_seed_hosts_are_normalized_and_seeds_skip_include():
    async def run():
        c = crawler.Crawler(["https://Books.Test/"], None, None, None,
                            max_pages=10, max_depth=1, per_host=1,
                            interval=0, include="/catalogue/")
        c.enqueue("https://Books.Test/", 0)
        return c.hosts, c.frontier.qsize()
    hosts, queued = asyncio.run(run())
    assert hosts == {"books.test"} and queued == 1


def test_escalation_learns_the_host_after_three_renders():
    shell = "<html><body><div id=root></div></body></html>"
    full = "<html><body><p>" + "word " * 200 + "</p></body></html>"

    class Fake:
        def __init__(self, html):
            self.html, self.calls = html, 0

        async def slots(self, n):
            return [self] * n

        async def fetch(self, url, validators=None):
            self.calls += 1
            return crawler.Page(200, url, self.html)

    async def run():
        http, tab = Fake(shell), Fake(full)
        tabs = asyncio.Queue()
        tabs.put_nowait(tab)
        c = crawler.Crawler(["https://a.test/"], http, None, io.StringIO(),
                            max_pages=100, max_depth=0, per_host=1,
                            interval=0, browsers=tabs)

        async def allowed(url):
            return True
        c.robots.allowed = allowed
        c.robots.crawl_delay = lambda url: None
        for i in range(10):
            c.enqueue(f"https://a.test/p{i}", 0)
        await asyncio.wait_for(c.run(1), 5)
        return http.calls, tab.calls
    assert asyncio.run(run()) == (3, 11)  # 3 escalations, then browser only


def test_checkpoint_is_atomic_and_flushes_records_first(tmp_path):
    out = open(tmp_path / "records.jsonl", "w")
    path = str(tmp_path / "state.json")
    c = crawler.Crawler(["https://a.test/"], None, None, out, max_pages=1,
                        max_depth=0, per_host=1, interval=0,
                        state={"https://a.test/": {"lastmod": "x"}},
                        state_path=path)
    out.write('{"url": "https://a.test/"}\n')  # still in the write buffer
    c.checkpoint()
    assert (tmp_path / "records.jsonl").read_text().count("\n") == 1
    assert json.load(open(path)) == {"https://a.test/": {"lastmod": "x"}}
    assert not (tmp_path / "state.json.tmp").exists()
    out.close()


def test_page_record_for_any_site():
    html = ('<html><head><title>Docs &amp; guides</title>'
            '<meta name="description" content="How it works">'
            '<link rel="canonical" href="https://a.test/docs/"></head>'
            '<body><h1>Getting started</h1></body></html>')
    rec = crawler.page_record("https://a.test/docs/", html)
    assert rec == {"url": "https://a.test/docs/", "type": "page",
                   "title": "Docs & guides", "h1": "Getting started",
                   "description": "How it works",
                   "canonical": "https://a.test/docs/"}


def test_local_browser_close_survives_a_failed_launch():
    asyncio.run(crawler.LocalBrowser(None).close())  # must not raise


def test_schema_org_subtypes_and_variant_prices():
    def rec(node):
        tree = LexborHTMLParser('<script type="application/ld+json">'
                                + json.dumps(node) + "</script>")
        return crawler.structured(tree, "u")
    news = rec({"@type": "ReportageNewsArticle", "headline": "H",
                "author": [{"@type": "Person", "name": "S"}]})
    assert news["type"] == "ReportageNewsArticle" and news["author"] == "S"
    group = rec({"@type": "ProductGroup", "name": "Shoe", "hasVariant": [
        {"@type": "Product", "offers": {"price": 105,
                                        "priceCurrency": "USD"}}]})
    assert (group["price"], group["currency"]) == (105, "USD")


def test_payment_required_records_the_price_and_never_escalates():
    class Tab:
        calls = 0

        async def fetch(self, url, validators=None):
            Tab.calls += 1
            return crawler.Page(200, url, "<html><body>paid</body></html>")

    async def run():
        async def paid(request):
            return web.Response(status=402, text="Payment required",
                                headers={"crawler-price": "USD 0.01"})
        app = web.Application()
        app.router.add_get("/{tail:.*}", paid)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        port = runner.addresses[0][1]
        tabs = asyncio.Queue()
        tabs.put_nowait(Tab())
        async with ClientSession() as http:
            c = crawler.Crawler([f"http://127.0.0.1:{port}/"],
                                crawler.HttpFetcher(http), http, io.StringIO(),
                                max_pages=10, max_depth=0, per_host=1,
                                interval=0, browsers=tabs)

            async def allowed(url):
                return True
            c.robots.allowed = allowed
            c.robots.crawl_delay = lambda url: None
            await asyncio.wait_for(c.run(1), 5)
        await runner.cleanup()
        return c
    c = asyncio.run(run())
    assert c.stats["payment_required"] == 1 and c.stats["records"] == 0
    assert list(c.prices.values()) == ["USD 0.01"]
    assert Tab.calls == 0  # a price is not a block: no browser retry


def test_markdown_runs_in_a_worker_process():
    from concurrent.futures import ProcessPoolExecutor
    article = ("<html><head><title>Async crawlers</title></head><body>"
               "<article><h1>Async crawlers</h1>"
               + "<p>A frontier schedules hosts, not jobs. " * 40
               + "</p></article></body></html>")

    class Fake:
        async def slots(self, n):
            return [self] * n

        async def fetch(self, url, validators=None):
            return crawler.Page(200, url, article)

    async def run(pool):
        out = io.StringIO()
        c = crawler.Crawler(["https://a.test/"], Fake(), None, out,
                            max_pages=1, max_depth=0, per_host=1, interval=0,
                            pages=True, markdown=pool)

        async def allowed(url):
            return True
        c.robots.allowed = allowed
        c.robots.crawl_delay = lambda url: None
        await asyncio.wait_for(c.run(1), 30)
        return json.loads(out.getvalue())
    with ProcessPoolExecutor(1) as pool:
        record = asyncio.run(run(pool))
    assert record["title"] == "Async crawlers"
    assert "A frontier schedules hosts" in record["markdown"]


def test_cloud_setup_mistakes_get_clear_messages():
    import os
    import subprocess
    import sys

    import gologin_pool
    env = {k: v for k, v in os.environ.items() if k != "GL_API_TOKEN"}
    run = subprocess.run(
        [sys.executable, "crawler.py", "--backend", "gologin"],
        capture_output=True, text=True, env=env)
    assert run.returncode == 2 and "set GL_API_TOKEN" in run.stderr
    run = subprocess.run(
        [sys.executable, "crawler.py", "--backend", "http",
         "--escalate", "gologin"],
        capture_output=True, text=True, env={**env, "GL_API_TOKEN": "t"})
    assert run.returncode == 2 and "pass --profiles" in run.stderr
    saved = os.environ.pop("GL_API_TOKEN", None)
    try:
        try:
            gologin_pool.api_token()
        except SystemExit as exit_:
            assert "GL_API_TOKEN" in str(exit_.code)
        else:
            raise AssertionError("expected SystemExit")
    finally:
        if saved is not None:
            os.environ["GL_API_TOKEN"] = saved


def test_recheck_sends_unchanged_lastmod_urls_again(monkeypatch):
    async def listed(http, robots, root):
        return {"https://a.test/p": "x"}
    monkeypatch.setattr(crawler, "sitemap_urls", listed)
    seen = {}
    for recheck in (False, True):
        c = crawler.Crawler(["https://a.test/"], None, None, io.StringIO(),
                            max_pages=5, max_depth=0, per_host=1, interval=0,
                            state={"https://a.test/p": {"lastmod": "x",
                                                        "etag": '"v1"'}},
                            recheck=recheck)
        asyncio.run(c.from_sitemaps())
        seen[recheck] = (c.stats["skipped_unchanged"], list(c.lastmod))
    assert seen[False] == (1, [])
    assert seen[True] == (0, ["https://a.test/p"])
