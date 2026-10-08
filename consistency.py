"""Preflight: does this browser tell one consistent story about itself?

uv run python consistency.py local-override  # Playwright, swapped User-Agent
uv run python consistency.py gologin <profile_id>
"""
import asyncio
import json
import os
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

from playwright.async_api import async_playwright

PROBE = """async () => {
  const ch = await navigator.userAgentData.getHighEntropyValues(
    ['platform', 'fullVersionList']);
  const gl = document.createElement('canvas').getContext('webgl');
  const dbg = gl.getExtension('WEBGL_debug_renderer_info');
  const ip = await (await fetch('https://ipinfo.io/json')).json();
  const echo = await (await fetch('https://httpbin.org/headers')).json();
  return {
    ua: navigator.userAgent,
    brands: navigator.userAgentData.brands.map(b => `${b.brand}/${b.version}`),
    webdriver: navigator.webdriver,
    ua_ch_platform: ch.platform,
    header_ch_platform: echo.headers['Sec-Ch-Ua-Platform'],
    navigator_platform: navigator.platform,
    software_gpu: /SwiftShader/i.test(
      gl.getParameter(dbg.UNMASKED_RENDERER_WEBGL)),
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone,
    ip_timezone: ip.timezone,
  };
}"""

OS_WORDS = {"Windows": "Win", "macOS": "Mac", "Linux": "Linux"}


def contradictions(s: dict) -> list[str]:
    found = []
    if s["webdriver"]:
        found.append("navigator.webdriver is true")
    if any(b.startswith("HeadlessChrome/") for b in s["brands"]):
        found.append("client hints announce HeadlessChrome")
    ua_major = s["ua"].split("Chrome/")[1].split(".")[0]
    if not any(b.endswith("/" + ua_major) for b in s["brands"]):
        found.append(f"User-Agent says Chrome {ua_major}, "
                     f"client hints say {', '.join(s['brands'])}")
    claimed = next((os for os, w in OS_WORDS.items() if w in s["ua"]), None)
    if claimed and s["ua_ch_platform"] != claimed:
        found.append(f"User-Agent says {claimed}, "
                     f"userAgentData says {s['ua_ch_platform']}")
    if claimed and claimed not in (s["header_ch_platform"] or ""):
        found.append(f"User-Agent says {claimed}, "
                     "Sec-CH-UA-Platform header says "
                     f"{s['header_ch_platform']}")
    if claimed and OS_WORDS[claimed] not in s["navigator_platform"]:
        found.append(f"User-Agent says {claimed}, "
                     f"navigator.platform says {s['navigator_platform']}")
    now = datetime.now()  # compare offsets: Asia/Calcutta is Asia/Kolkata
    if (ZoneInfo(s["timezone"]).utcoffset(now)
            != ZoneInfo(s["ip_timezone"]).utcoffset(now)):
        found.append(f"clock says {s['timezone']}, IP says {s['ip_timezone']}")
    if s["software_gpu"]:
        found.append("WebGL renders on SwiftShader, a software GPU")
    return found


async def main() -> None:
    async with async_playwright() as pw:
        if sys.argv[1] == "local-override":
            # channel="chromium": the full browser, headless, instead of the
            # lighter headless shell
            browser = await pw.chromium.launch(channel="chromium",
                                               headless=True)
            ctx = await browser.new_context(user_agent=(
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/155.0.0.0 Safari/537.36"))
        else:
            url = ("wss://cloudbrowser.gologin.com/connect?token="
                   f"{os.environ['GL_API_TOKEN']}&profile={sys.argv[2]}")
            browser = await pw.chromium.connect_over_cdp(url, no_defaults=True)
            ctx = browser.contexts[0]
        page = await ctx.new_page()
        await page.goto("https://httpbin.org/html")
        signals = await page.evaluate(PROBE)
        print(json.dumps(signals, indent=2))
        print("contradictions:", contradictions(signals) or "none")
        await page.close()
        await browser.close()


asyncio.run(main())
