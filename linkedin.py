import os
import time
from dotenv import load_dotenv
from playwright.sync_api import sync_playwright
load_dotenv()
EMAIL = os.getenv("LINKEDIN_EMAIL")
PASSWORD = os.getenv("LINKEDIN_PASSWORD")
PROFILE_NAME = os.getenv("PROFILE_NAME")
def scrape_linkedin():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)
        page = browser.new_page()
        page.goto("https://www.linkedin.com/login")
        page.fill("#username", EMAIL)
        page.fill("#password", PASSWORD)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")
        search_url = f"https://www.linkedin.com/search/results/people/?keywords={PROFILE_NAME}&network=%5B%22F%22%5D"
        page.goto(search_url)
        page.wait_for_timeout(5000)
        first_result = page.locator("a.app-aware-link").first
        try:
            first_result.wait_for(timeout=15000)
            first_result.click()
        except:
            print("No matching connection found.")
            browser.close()
            return
if __name__ == "__main__":
    scrape_linkedin()
