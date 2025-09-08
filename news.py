import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
import pandas as pd


tickers = [
    "AAPL", "AAL.L", "BHP", "GLEN.L", "VALE", "BOL.ST", "KGHA.F",
    "TECK", "LUN.TO", "RIO", "GMEXICOB.MX", "2899.HK", "FCX",
    "FM.TO", "0847.HK", "BVN", "CVERDEC1.LM", "FLS", "M6QB.F", "601608.SS"
]

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


def scrape_yahoo_news_links(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}/news/"
    driver = get_chrome_driver()
    driver.get(url)
    time.sleep(5) 

    news_list = [] 
    articles = driver.find_elements(By.CSS_SELECTOR, "a.subtle-link")
    for article in articles:
        title = article.text.strip()
        href = article.get_attribute("href")
        if title and href and "/news/" in href:
            if not href.startswith("http"):
                href = "https://finance.yahoo.com" + href
            news_list.append({"title": title, "link": href})

    driver.quit()
    return news_list  


if __name__ == "__main__":
    all_news = []

    for ticker in tickers:
        print(f"Fetching news links for {ticker}...")
        try:
            links = scrape_yahoo_news_links(ticker)
            for news in links:
                all_news.append({
                    "ticker": ticker,
                    "news_title": news["title"],
                    "news_link": news["link"]
                })
        except Exception as e:
            print(f"Error fetching {ticker}: {e}")

 
    df = pd.DataFrame(all_news)
    df.to_csv("yahoo_news_links_with_titles.csv", index=False)
    print("\n Done! Saved all news links with titles to yahoo_news_links_with_titles.csv")
