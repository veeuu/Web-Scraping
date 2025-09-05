import time
import yfinance as yf
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


def scrape_yahoo_links_selenium(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}"
    
    chrome_options = Options()
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-allow-origins=*")

    service = Service()
    driver = webdriver.Chrome(service=service, options=chrome_options)

    driver.get(url)
    time.sleep(5)

    nav_links = {}
    valid_tabs = [
        "Summary", "News", "Chart", "Conversations", "Statistics",
        "Historical Data", "Profile", "Financials", "Analysis",
        "Options", "Holders", "Sustainability"
    ]

    elements = driver.find_elements(By.CSS_SELECTOR, "ul li a")
    for el in elements:
        text = el.text.strip()
        href = el.get_attribute("href")
        if text in valid_tabs:
            nav_links[text] = href

    driver.quit()
    return nav_links


def get_stock_data(ticker):
    stock = yf.Ticker(ticker)
    data = {
        "current_price": stock.history(period="1d")["Close"].iloc[-1],
        "market_cap": stock.info.get("marketCap"),
        "pe_ratio": stock.info.get("trailingPE"),
        "beta": stock.info.get("beta"),
    }
    return data


if __name__ == "__main__":
    ticker = "AAPL"

    links = scrape_yahoo_links_selenium(ticker)
    print("📌 Yahoo Finance Navigation Links:")
    for name, url in links.items():
        print(f"{name}: {url}")

    print("\n📌 Stock Data:")
    stock_info = get_stock_data(ticker)
    for key, value in stock_info.items():
        print(f"{key}: {value}")
