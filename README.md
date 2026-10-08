# async-web-crawler

Companion code for the article
[How to Make a Web Crawler That Scales With Async Python](https://gologin.com/blog/how-to-make-a-web-crawler/).
The article explains every design choice and shows the measurements. This repo
is the runnable version.

One asyncio process with:

- **A per-host frontier.** One queue per host and a per-host rate limit.
  Workers take jobs only from hosts that are ready, so a slow or rate-limited
  site doesn't delay the others.
- **Politeness.** `robots.txt` through [Protego](https://github.com/scrapy/protego)
  with RFC 9309 status handling, `Crawl-delay`, a per-host interval and
  concurrency cap, and `Retry-After` pauses (capped at 5 minutes). A
  `402 Payment Required` from Pay Per Crawl is recorded with its
  `crawler-price`, never retried in a browser.
- **Sitemap-first discovery** with `lastmod`, size-capped against gzip bombs.
- **Structured data first.** schema.org JSON-LD articles and products of any
  subtype (`ReportageNewsArticle`, Shopify's `ProductGroup` with variant
  prices), then per-site CSS selectors, then `--pages` for everything else.
- **Markdown for LLMs.** `--markdown` adds each page's main content as
  markdown (trafilatura), extracted in a process pool so the event loop
  keeps fetching.
- **Incremental recrawls.** A state file skips unchanged `lastmod` dates and
  sends `If-Modified-Since` / `If-None-Match` for the rest. `--recheck`
  sends a conditional GET for every URL, for sites whose `lastmod` lags.
- **3 fetchers.** aiohttp, local Chromium, or
  [Gologin Cloud Browser](https://gologin.com/cloud-browser/) profiles over the
  Chrome DevTools Protocol (CDP).
  With `--escalate`, HTTP fetches first and sends the pages it can't read, such
  as app shells, to a small browser pool. robots.txt still applies.

## Quickstart

Requires Python 3.13+ (3.14 for `python -m asyncio ps`) and
[uv](https://docs.astral.sh/uv/).

```bash
uv sync
uv run playwright install --with-deps chromium   # --with-deps: Linux system libraries
uv run python crawler.py --backend http --max-pages 200 --interval 0
```

Before you crawl a site you don't own, pass `--user-agent` with a page that
says who runs the crawler, and keep `--interval` at 1 second or more.

## Common runs

```bash
# Your own site: url/title/h1/description and main-content markdown per page
uv run python crawler.py --backend http --per-host 1 --interval 1 \
  --seed https://example.com/ --pages --markdown --out pages.jsonl \
  --user-agent "MyCrawler/1.0 (+https://example.com/about-my-crawler)"

# Several sites at once; the seeds' hosts are the crawl scope
uv run python crawler.py --backend http --per-host 1 --interval 1 \
  --seed https://books.toscrape.com/ https://quotes.toscrape.com/

# Sitemap-first, incremental: the second run fetches only what changed
uv run python crawler.py --backend http --sitemap --max-depth 0 \
  --seed https://example.com/ --include '/blog/' --state state.json --out posts.jsonl

# HTTP first, local browser for pages that need JavaScript
uv run python crawler.py --backend http --escalate local --tabs 2 \
  --seed https://quotes.toscrape.com/js/ --include '/js/' --out quotes.jsonl

# Browser backend on Gologin Cloud Browser profiles
cp .env.example .env  # add your token
set -a && . ./.env && set +a
uv run python gologin_pool.py 3 > profiles.txt   # create profiles once
uv run python crawler.py --backend gologin --profiles "$(cat profiles.txt)" \
  --tabs 16 --per-host 48 --interval 0
uv run python consistency.py gologin "$(cut -d, -f1 profiles.txt)"

# HTTP first, cloud profile only for pages HTTP can't read
uv run python crawler.py --backend http --escalate gologin \
  --profiles "$(cut -d, -f1 profiles.txt)" --tabs 2 \
  --seed https://quotes.toscrape.com/js/ --include '/js/' --out quotes.jsonl
```

`uv run python crawler.py --help` lists every flag.

`--backend gologin` stops every session it opened on exit. Create profiles
once and reuse their IDs, so every crawl comes back as the same browsers.

## Files

| File | What it is |
|---|---|
| `crawler.py` | Frontier, robots, sitemaps, parser, classifier, HTTP and local fetchers |
| `gologin_pool.py` | Cloud Browser fetcher: connect, tabs, stop |
| `consistency.py` | Checks a browser for self-contradicting signals |
| `tests/` | Offline tests: no network, browser, or token |
| `benchmarks/frontier_ab.py` | The 5-site test of the frontier against a single queue |

```bash
uv run pytest -q
```

## License

MIT. See [LICENSE](LICENSE).
