import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By

def get_driver():
    options = Options()
    options.add_argument("--headless=new")
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    return webdriver.Chrome(service=Service(), options=options)

def scrape_analysis_tab(ticker):
    url = f"https://finance.yahoo.com/quote/{ticker}/analysis"
    driver = get_driver()
    driver.get(url)
    time.sleep(5)  # wait for tables to load

    # grab all table elements
    tables = driver.find_elements(By.TAG_NAME, "table")
    dataframes = []

    for table in tables:
        html = table.get_attribute("outerHTML")
        try:
            df = pd.read_html(html)[0]
            dataframes.append(df)
        except:
            continue

    driver.quit()
    return dataframes

if __name__ == "__main__":
    ticker = "AAPL"
    dfs = scrape_analysis_tab(ticker)
    for i, df in enumerate(dfs):
        print(f"\n📌 Table {i+1}")
        print(df.head())
