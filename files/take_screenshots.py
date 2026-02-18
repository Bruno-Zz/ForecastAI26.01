"""
Take screenshots of the ForecastAI dashboard and detail views using Playwright.
Saves screenshots to files/screenshots/ for inclusion in documentation.
"""
import asyncio
import os
import json
import urllib.request

SCREENSHOTS_DIR = os.path.join(os.path.dirname(__file__), "screenshots")
os.makedirs(SCREENSHOTS_DIR, exist_ok=True)

BASE_URL = "http://localhost:5173"
API_URL = "http://localhost:8000"

def find_good_series():
    """Find a series with metrics (36+ obs) for the detail screenshot."""
    resp = urllib.request.urlopen(f"{API_URL}/api/series?limit=50000")
    data = json.loads(resp.read().decode())
    long_series = [s for s in data if s["n_observations"] >= 36]
    if long_series:
        # prefer one with seasonality or trend
        for s in long_series:
            if s.get("has_seasonality") or s.get("has_trend"):
                return s["unique_id"]
        return long_series[0]["unique_id"]
    # fallback to any series
    return data[0]["unique_id"] if data else None


async def take_screenshots():
    from playwright.async_api import async_playwright

    series_id = find_good_series()
    print(f"Using series: {series_id}")

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        context = await browser.new_context(
            viewport={"width": 1440, "height": 900},
            device_scale_factor=2,  # retina-quality
        )
        page = await context.new_page()

        # --- Screenshot 1: Dashboard ---
        print("Navigating to Dashboard...")
        await page.goto(BASE_URL, wait_until="networkidle", timeout=30000)
        await page.wait_for_timeout(3000)  # let charts render
        path1 = os.path.join(SCREENSHOTS_DIR, "01_dashboard.png")
        await page.screenshot(path=path1, full_page=True)
        print(f"  Saved: {path1}")

        # --- Screenshot 2: Dashboard - scrolled to table ---
        print("Scrolling to series table...")
        await page.evaluate("window.scrollTo(0, 600)")
        await page.wait_for_timeout(1000)
        path2 = os.path.join(SCREENSHOTS_DIR, "02_dashboard_table.png")
        await page.screenshot(path=path2)
        print(f"  Saved: {path2}")

        # --- Screenshot 3: Series detail - main chart ---
        if series_id:
            encoded = urllib.parse.quote(series_id, safe="")
            detail_url = f"{BASE_URL}/series/{encoded}"
            print(f"Navigating to series detail: {detail_url}")
            await page.goto(detail_url, wait_until="networkidle", timeout=30000)
            await page.wait_for_timeout(4000)  # let Vega charts render
            path3 = os.path.join(SCREENSHOTS_DIR, "03_series_header_chart.png")
            await page.screenshot(path=path3)
            print(f"  Saved: {path3}")

            # --- Screenshot 4: Full page detail ---
            path4 = os.path.join(SCREENSHOTS_DIR, "04_series_full.png")
            await page.screenshot(path=path4, full_page=True)
            print(f"  Saved: {path4}")

            # --- Screenshot 5: Metrics section ---
            print("Scrolling to metrics...")
            await page.evaluate("window.scrollTo(0, 750)")
            await page.wait_for_timeout(1000)
            path5 = os.path.join(SCREENSHOTS_DIR, "05_series_metrics.png")
            await page.screenshot(path=path5)
            print(f"  Saved: {path5}")

            # --- Screenshot 6: Racing bars / forecast evolution ---
            print("Scrolling to forecast evolution...")
            await page.evaluate("window.scrollTo(0, 1400)")
            await page.wait_for_timeout(1000)
            path6 = os.path.join(SCREENSHOTS_DIR, "06_series_evolution.png")
            await page.screenshot(path=path6)
            print(f"  Saved: {path6}")

            # --- Screenshot 7: Forecast values table ---
            print("Scrolling to forecast table...")
            await page.evaluate("window.scrollTo(0, document.body.scrollHeight)")
            await page.wait_for_timeout(1000)
            path7 = os.path.join(SCREENSHOTS_DIR, "07_series_forecast_table.png")
            await page.screenshot(path=path7)
            print(f"  Saved: {path7}")

        await browser.close()
    print("\nAll screenshots saved to:", SCREENSHOTS_DIR)


if __name__ == "__main__":
    import urllib.parse
    asyncio.run(take_screenshots())
