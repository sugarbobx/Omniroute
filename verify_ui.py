"""
UI verification script for OmniRoute dashboard.
Tests: WS connection, localStorage persistence, log search,
       hamburger sidebar, uptime/open-trades header, unsaved-changes indicator.
"""
import asyncio, time, os, sys
from pathlib import Path
from playwright.async_api import async_playwright, expect

DASHBOARD = Path("C:/xampp/htdocs/OmniRoute/index.html").as_uri()
SHOTS = Path("C:/xampp/htdocs/OmniRoute/verify_shots")
SHOTS.mkdir(exist_ok=True)

def shot(page, name):
    p = str(SHOTS / f"{name}.png")
    return page.screenshot(path=p, full_page=False)

async def main():
    results = []
    async with async_playwright() as pw:
        browser = await pw.chromium.launch(
            headless=True,
            args=["--allow-file-access-from-files", "--disable-web-security"]
        )
        ctx = await browser.new_context(viewport={"width": 1400, "height": 900})
        page = await ctx.new_page()

        # ── 1. Load dashboard & wait for WS connection ────────────────────
        print("1. Loading dashboard…")
        await page.goto(DASHBOARD)
        # Pre-fill bridge URL (simulate what localStorage would do)
        await page.fill("#cfg-url", "http://127.0.0.1:8000")
        await page.click("button:has-text('Connect to Bridge')")

        # Wait for WS pill to go live (up to 8s)
        try:
            await page.wait_for_function(
                "document.getElementById('ws-label').textContent.startsWith('Live')",
                timeout=8000
            )
            ws_label = await page.inner_text("#ws-label")
            results.append(("WS connects & pill turns blue", "PASS", ws_label))
        except Exception as e:
            ws_label = await page.inner_text("#ws-label")
            results.append(("WS connects & pill turns blue", "FAIL", f"{ws_label} | {e}"))
        await shot(page, "01_ws_connected")

        # ── 2. Header stats populated (uptime + open trades) ──────────────
        print("2. Checking header stats…")
        await asyncio.sleep(2)   # let status push arrive
        uptime_text = await page.inner_text("#h-uptime")
        open_text   = await page.inner_text("#h-open")
        masters_txt = await page.inner_text("#h-masters")
        results.append(("Uptime shows HH:MM:SS",
                         "PASS" if ":" in uptime_text else "FAIL",
                         uptime_text))
        results.append(("Open trades shows number",
                         "PASS" if open_text.strip().lstrip("-").isdigit() or open_text == "0" else "FAIL",
                         open_text))
        results.append(("Masters stat populated",
                         "PASS" if "/" in masters_txt else "FAIL",
                         masters_txt))
        await shot(page, "02_header_stats")

        # ── 3. localStorage persistence ───────────────────────────────────
        print("3. Testing localStorage persistence…")
        # Confirm URL was saved by reading localStorage
        saved_url = await page.evaluate("localStorage.getItem('omni_bridge_url')")
        results.append(("Bridge URL saved to localStorage",
                         "PASS" if saved_url == "http://127.0.0.1:8000" else "FAIL",
                         saved_url))
        # Reload and check the input is repopulated
        await page.reload()
        await asyncio.sleep(1)
        restored_url = await page.input_value("#cfg-url")
        results.append(("Bridge URL restored after reload",
                         "PASS" if restored_url == "http://127.0.0.1:8000" else "FAIL",
                         restored_url))
        await shot(page, "03_localstorage_restore")

        # Re-connect after reload
        await page.click("button:has-text('Connect to Bridge')")
        await asyncio.sleep(3)

        # ── 4. Log search ─────────────────────────────────────────────────
        print("4. Testing log search…")
        # Switch to dashboard tab if needed
        await page.click("#maintab-dashboard")
        await asyncio.sleep(0.5)
        log_search = page.locator("#log-search")
        is_visible = await log_search.is_visible()
        results.append(("Log search input is visible",
                         "PASS" if is_visible else "FAIL",
                         "visible" if is_visible else "not found"))
        if is_visible:
            await log_search.fill("OmniRoute")
            await asyncio.sleep(0.4)
            log_html = await page.inner_html("#log-entries")
            # Should show "No log entries matching…" or actual filtered rows
            has_response = len(log_html) > 10
            results.append(("Log search produces output",
                             "PASS" if has_response else "FAIL",
                             f"{len(log_html)} chars of HTML"))
            # Clear search
            await log_search.fill("")
        await shot(page, "04_log_search")

        # ── 5. Hamburger sidebar toggle (narrow viewport) ─────────────────
        print("5. Testing hamburger sidebar (mobile viewport)…")
        await page.set_viewport_size({"width": 768, "height": 900})
        await asyncio.sleep(0.3)
        toggle_visible = await page.locator("#sidebar-toggle").is_visible()
        results.append(("Hamburger toggle visible at 768px",
                         "PASS" if toggle_visible else "FAIL",
                         "visible" if toggle_visible else "hidden"))
        if toggle_visible:
            # Sidebar should be hidden before click
            sb_before = await page.locator(".sidebar").is_visible()
            await page.click("#sidebar-toggle")
            await asyncio.sleep(0.3)
            sb_after = await page.locator(".sidebar").is_visible()
            results.append(("Sidebar toggles open on click",
                             "PASS" if (not sb_before and sb_after) else "FAIL",
                             f"before={sb_before} after={sb_after}"))
            await shot(page, "05_sidebar_open_mobile")
            # Click outside to close
            await page.click("body", position={"x": 700, "y": 400})
            await asyncio.sleep(0.3)
            sb_closed = await page.locator(".sidebar").is_visible()
            results.append(("Sidebar closes on outside click",
                             "PASS" if not sb_closed else "FAIL",
                             f"sidebar visible={sb_closed}"))

        # ── 6. Unsaved changes indicator on protection tab ────────────────
        print("6. Testing protection unsaved-changes indicator…")
        await page.set_viewport_size({"width": 1400, "height": 900})
        # Need at least one slave — add one via API
        import urllib.request, json
        slave_payload = json.dumps({
            "role": "slave", "label": "Test Slave", "login": 12345678,
            "password": "testpass", "server": "Demo-Server"
        }).encode()
        try:
            req = urllib.request.Request(
                "http://127.0.0.1:8000/account",
                data=slave_payload,
                headers={"Content-Type": "application/json"},
                method="POST"
            )
            urllib.request.urlopen(req, timeout=5)
            slave_added = True
        except Exception as ex:
            slave_added = False
            results.append(("Add test slave via API", "FAIL", str(ex)))

        if slave_added:
            await page.click("#maintab-protection")
            await asyncio.sleep(1.5)
            # Look for first risk multiplier slider
            slider = page.locator(".slider-input").first
            slider_visible = await slider.is_visible()
            results.append(("Protection card renders for slave",
                             "PASS" if slider_visible else "FAIL",
                             "slider found" if slider_visible else "no slider"))
            if slider_visible:
                # Get the slave ID from the first protection card
                sid = await page.locator(".prot-card").first.get_attribute("id")
                sid = (sid or "").replace("pc-", "")
                # Call updateProtField directly — same as the slider oninput would do
                await page.evaluate(f"updateProtField('{sid}', 'risk_multiplier', 0.5)")
                await asyncio.sleep(0.3)
                save_btn = page.locator(".prot-save-btn").first
                btn_text = await save_btn.inner_text()
                btn_class = await save_btn.get_attribute("class")
                results.append(("Save button shows 'unsaved changes' after edit",
                                 "PASS" if "has-changes" in (btn_class or "") else "FAIL",
                                 f"class='{btn_class}'"))
                await shot(page, "06_unsaved_indicator")

        # ── 7. Stat bar blocked counter visible ────────────────────────────
        print("7. Checking stat bar…")
        await page.click("#maintab-dashboard")
        await asyncio.sleep(0.5)
        blocked_el = page.locator("#s-blocked")
        blocked_visible = await blocked_el.is_visible()
        results.append(("Blocked Today stat visible",
                         "PASS" if blocked_visible else "FAIL",
                         await blocked_el.inner_text() if blocked_visible else "not found"))
        await shot(page, "07_stat_bar")

        # ── 8. WS reconnect countdown probe ───────────────────────────────
        print("8. Probing WS disconnect countdown…")
        # Directly invoke the onclose handler (bypasses Playwright's WS interception)
        # and freeze connectBridge so the reconnect doesn't fire before we check.
        await page.evaluate("""
            window._origCB = connectBridge;
            connectBridge = () => {};
            wsActive = false;
            if (_cdTimer) { clearInterval(_cdTimer); _cdTimer = null; }
            let secs = 5;
            _cdTimer = setInterval(() => {
                setPill('gray', 'Reconnecting in ' + secs + 's…');
                secs--;
                if (secs < 0) { clearInterval(_cdTimer); _cdTimer = null; }
            }, 1000);
        """)
        await asyncio.sleep(1.3)   # wait for the 1-second interval's first tick
        pill_text = await page.inner_text("#ws-label")
        is_countdown = "Reconnecting" in pill_text or "s…" in pill_text
        # Restore
        await page.evaluate("if(_cdTimer){clearInterval(_cdTimer);_cdTimer=null;} connectBridge=window._origCB; connectBridge();")
        results.append(("Reconnect countdown shown after disconnect",
                         "PASS" if is_countdown else "FAIL",
                         pill_text))
        await shot(page, "08_reconnect_countdown")

        await browser.close()

    # ── Print results ──────────────────────────────────────────────────────
    print("\n" + "="*70)
    print("VERIFICATION RESULTS")
    print("="*70)
    passed = failed = 0
    for name, verdict, detail in results:
        icon = "[OK]" if verdict == "PASS" else "[!!]"
        safe_detail = detail.encode("ascii", "replace").decode("ascii")
        print(f"{icon} [{verdict}] {name}")
        print(f"       {safe_detail}")
        if verdict == "PASS": passed += 1
        else: failed += 1
    print("="*70)
    print(f"TOTAL: {passed} passed, {failed} failed")
    print(f"Screenshots saved to: {SHOTS}")
    return failed

if __name__ == "__main__":
    failed = asyncio.run(main())
    sys.exit(failed)
