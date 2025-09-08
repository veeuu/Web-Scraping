import time
import yfinance as yf
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options


def get_chrome_driver():
    chrome_options = Options()
    chrome_options.add_argument("--ignore-certificate-errors")
    chrome_options.add_argument("--ignore-ssl-errors")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--remote-allow-origins=*")
    chrome_options.add_argument("--headless=new")  

    service = Service()
    return webdriver.Chrome(service=service, options=chrome_options)


def scrape_yahoo_links_selenium(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}"
    driver = get_chrome_driver()
    driver.get(url)
    time.sleep(5)

    nav_links = {}
    valid_tabs = [
        "Summary", "Chart", "Conversations", "Statistics",
        "Historical Data", "Profile", "Financials", "Analysis",
        "Options", "Holders", "Sustainability"
    ]

   
    elements = driver.find_elements(By.CSS_SELECTOR, "ul li a")
    for el in elements:
        text = el.text.strip()
        href = el.get_attribute("href")
        if text in valid_tabs and href:
            nav_links[text] = href

    driver.quit()
    return nav_links


def scrape_yahoo_news_links(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}/news/"
    driver = get_chrome_driver()
    driver.get(url)
    time.sleep(5)

    news_links = set() 
    articles = driver.find_elements(By.CSS_SELECTOR, "a.subtle-link")
    for article in articles:
        href = article.get_attribute("href")
        if href and "/news/" in href:
            if not href.startswith("http"):
                href = "https://finance.yahoo.com" + href
            news_links.add(href) 

    driver.quit()
    return list(news_links)  

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
    stock_info = get_stock_data(ticker)
    news_links = scrape_yahoo_news_links(ticker)

    print("📌 Yahoo Finance Navigation Links:")
    for name, url in links.items():
        print(f"{name}: {url}")

    print("\n📌 Stock Data:")
    for key, value in stock_info.items():
        print(f"{key}: {value}")

    print("\n📌 Yahoo Finance News Links:")
    for link in news_links:
        print(link)
