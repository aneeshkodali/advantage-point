from playwright.sync_api import (
    Page,
    sync_playwright
)

def create_playwright_page():
    playwright = sync_playwright().start()
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()
    return playwright, browser, page  # you'll need these to close later