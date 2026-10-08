"""Fetcher that runs every tab inside Gologin Cloud Browser profiles.

One profile = one identity: fingerprint, cookies and storage, and the proxy
attached to it, if any.
"""
import asyncio
import os
import sys

import aiohttp

from crawler import BrowserSlot

API = "https://api.gologin.com"
CONNECT = "https://cloudbrowser.gologin.com/connect?token={}&profile={}"
# Cap the preflight at 60 s instead of aiohttp's 5-minute default
PREFLIGHT = aiohttp.ClientTimeout(total=60)
CONNECT_TIMEOUT_MS = 120_000


class CloudSetupError(RuntimeError):
    """A token or profile problem the user can fix (no traceback)."""


def api_token() -> str:
    """The Gologin API token, or a clear message about where to get one."""
    token = os.environ.get("GL_API_TOKEN")
    if not token:
        sys.exit("Set GL_API_TOKEN to your Gologin API token "
                 "(dashboard > API & MCP > API).")
    return token


async def create_profiles(http: aiohttp.ClientSession, n: int,
                          prefix: str = "crawler") -> list[str]:
    """Run once, store the IDs, and reuse them on every crawl."""
    token = api_token()
    ids = []
    for i in range(n):
        async with http.post(f"{API}/browser/quick",
                             headers={"Authorization": f"Bearer {token}"},
                             json={"name": f"{prefix}-{i + 1}",
                                   "os": "lin"}) as r:
            r.raise_for_status()
            ids.append((await r.json())["id"])
    return ids


class GologinPool:
    def __init__(self, pw, http: aiohttp.ClientSession, profiles: list[str],
                 tabs: int, settle: float = 5.0):
        self.pw, self.http, self.profiles, self.tabs = pw, http, profiles, tabs
        self.settle = settle
        self.token = api_token()
        self.browsers = []

    async def _preflight(self, url: str, profile_id: str) -> None:
        """A plain GET first, so the API's x-error-reason reaches you."""
        async with self.http.get(url, timeout=PREFLIGHT) as pre:
            if pre.status >= 400:
                reason = (pre.headers.get("x-error-reason")
                          or (await pre.text())[:200])
                if "Unauthorized" in reason:
                    raise CloudSetupError("The API token was rejected. "
                                          "Check GL_API_TOKEN.")
                if '"statusCode":404' in reason.replace(" ", ""):
                    raise CloudSetupError(f"Profile {profile_id!r} wasn't "
                                          "found. Check profiles.txt.")
                raise CloudSetupError(
                    f"Couldn't open profile {profile_id!r} ({pre.status}): "
                    f"{reason}")

    async def _open(self, profile_id: str) -> list[BrowserSlot]:
        url = CONNECT.format(self.token, profile_id)
        await self._preflight(url, profile_id)
        browser = await self.pw.chromium.connect_over_cdp(
            url.replace("https://", "wss://"), timeout=CONNECT_TIMEOUT_MS,
            no_defaults=True)  # keep the profile's own settings
        self.browsers.append((profile_id, browser))
        ctx = browser.contexts[0]  # the profile's own context and storage
        pages = list(ctx.pages)
        while len(pages) < self.tabs:
            pages.append(await ctx.new_page())
        slots = [BrowserSlot(p) for p in pages[: self.tabs]]
        for s in slots:
            await s.prepare()
        return slots

    async def slots(self, n: int) -> list[BrowserSlot]:
        print(f"Connecting {len(self.profiles)} cloud profile(s)...",
              file=sys.stderr, flush=True)
        groups = await asyncio.gather(*(self._open(p) for p in self.profiles))
        return [slot for group in groups for slot in group]

    async def close(self) -> None:
        print("Saving profiles and stopping cloud sessions...",
              file=sys.stderr, flush=True)
        # Let Chromium save its recent cookie and storage writes to the profile
        await asyncio.sleep(self.settle)
        for _, browser in self.browsers:
            try:
                await browser.close()
            except Exception:  # the driver may already be gone (Ctrl+C)
                pass
        # End each session through the API, like the Cloud Browser quickstart
        await asyncio.gather(*(self._stop(pid) for pid, _ in self.browsers),
                             return_exceptions=True)

    async def _stop(self, profile_id: str) -> int:
        async with self.http.delete(
                f"{API}/browser/{profile_id}/web",
                headers={"Authorization": f"Bearer {self.token}"}) as r:
            return r.status


if __name__ == "__main__":  # uv run python gologin_pool.py 3 [name-prefix]
    async def _create(n: int, prefix: str) -> None:
        async with aiohttp.ClientSession() as http:
            print(",".join(await create_profiles(http, n, prefix)))

    asyncio.run(_create(int(sys.argv[1]) if len(sys.argv) > 1 else 3,
                        sys.argv[2] if len(sys.argv) > 2 else "crawler"))
