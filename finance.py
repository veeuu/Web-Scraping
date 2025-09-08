from playwright.sync_api import sync_playwright
from bs4 import BeautifulSoup

def scrape_aapl_profile():
    url = "https://finance.yahoo.com/quote/AAPL/profile/"

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url)

        # Wait a bit to ensure the page loads (profile is JS-rendered)
        page.wait_for_timeout(5000)  # 5 seconds

        # Get full page content
        content = page.content()
        browser.close()

    soup = BeautifulSoup(content, 'html.parser')

    # Extract company description
    description_tag = soup.find('section', {'data-test': 'qsp-profile'})
    if description_tag:
        profile_data = {}
        # Company description
        desc = description_tag.find('p')
        profile_data['Description'] = desc.get_text(strip=True) if desc else None

        # Company details: Sector, Industry, Employees
        rows = description_tag.find_all('span', class_='Fw(600)')
        for span in rows:
            label = span.get_text(strip=True).replace(":", "")
            value = span.find_next_sibling(text=True)
            if value:
                profile_data[label] = value.strip()

        # Website
        website_tag = description_tag.find('a', href=True)
        if website_tag:
            profile_data['Website'] = website_tag['href']

        return profile_data
    else:
        return None

if __name__ == "__main__":
    profile = scrape_aapl_profile()
    if profile:
        for key, value in profile.items():
            print(f"{key}: {value}")
    else:
        print("Failed to retrieve profile data.")
