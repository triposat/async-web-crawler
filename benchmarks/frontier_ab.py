"""Five sites at once: the old single-queue design vs the per-host Frontier.

The old design (crawler.py before the frontier change) took the next job from
one global priority queue, then slept in the per-host gate while holding it.
NaiveFrontier reproduces exactly that, behind the Frontier interface.

uv run python benchmarks/frontier_ab.py naive|frontier [fixed]

"fixed" crawls the same 100 URLs (20 per site, listed site by site, as link
discovery fills a queue) with no link-following, so both designs do equal work.
"""
import asyncio, collections, heapq, json, sys, time
from urllib.parse import urlsplit
import aiohttp
sys.path.insert(0, ".")
import crawler

SEEDS = ["https://books.toscrape.com/", "https://quotes.toscrape.com/",
         "https://www.scrapethissite.com/pages/",
         "https://docs.python.org/3/library/",
         "https://gologin.com/blog/"]


class NaiveFrontier(crawler.Frontier):
    """One global heap; get() waits for the host while holding the job."""

    def __init__(self, per_host, interval):
        super().__init__(per_host, interval)
        self.heap, self.sems, self.has_job = [], {}, asyncio.Event()

    def qsize(self):
        return len(self.heap)

    def _push(self, job):
        heapq.heappush(self.heap, job)
        self.has_job.set()

    async def get(self):
        while not self.heap:
            if self.closed:
                raise asyncio.QueueShutDown
            self.has_job.clear()
            await self.has_job.wait()
        job = heapq.heappop(self.heap)
        host = urlsplit(job.url).netloc
        sem = self.sems.setdefault(host, asyncio.Semaphore(self.per_host))
        await sem.acquire()  # the old HostGate.acquire()
        now = time.monotonic()
        start = max(now, self.next_at.get(host, 0),
                    self.paused_until.get(host, 0))
        self.next_at[host] = start + self.intervals.get(host, self.interval)
        await asyncio.sleep(start - now)
        return job

    def task_done(self, job):
        self.sems[urlsplit(job.url).netloc].release()
        self.unfinished -= 1
        if self.unfinished == 0:
            self.idle.set()

    def shutdown(self):
        self.closed = True
        self.has_job.set()


async def main(kind):
    hdr = {"User-Agent": crawler.USER_AGENT}
    async with aiohttp.ClientSession(headers=hdr) as http:
        with open(f"benchmarks/records_{kind}.jsonl", "w") as out:
            fixed = len(sys.argv) > 2
            seeds = (open("benchmarks/fixed_urls.txt").read().split()
                     if fixed else SEEDS)
            c = crawler.Crawler(seeds, crawler.HttpFetcher(http), http, out,
                                max_pages=10_000 if fixed else 300,
                                max_depth=0 if fixed else 3, per_host=1,
                                interval=1.0)
            if kind == "naive":
                c.frontier = NaiveFrontier(1, 1.0)
            done_at = []
            handle = c.handle
            async def timed(slot, job):
                await handle(slot, job)
                done_at.append((time.monotonic(), urlsplit(job.url).netloc))
            c.handle = timed
            elapsed = await c.run(16)
    t0 = c.started
    per_host = collections.Counter(h for _, h in done_at)
    finished = {h: round(max(t for t, x in done_at if x == h) - t0, 1)
                for h in per_host}
    first_minute = collections.Counter(h for t, h in done_at if t - t0 <= 60)
    print(json.dumps({"design": kind, "workers": 16,
                      "seconds": round(elapsed, 1),
                      "pages": len(done_at),
                      "pages_per_s": round(len(done_at) / elapsed, 2),
                      "first_60s_by_host": dict(first_minute),
                      "by_host": dict(per_host),
                      "host_finished_at_s": finished,
                      "stats": dict(c.stats)}))

asyncio.run(main(sys.argv[1]))
