import yfinance as yf
import pandas as pd

# Define the ticker
ticker = "AAPL"

# Fetch the stock data
stock = yf.Ticker(ticker)

# Extract the stock info
info = stock.info

# Create a DataFrame for the stock summary
stock_summary = {
    "Name": [info.get("longName", "N/A")],
    "Symbol": [ticker],
    "Current Price (USD)": [info.get("regularMarketPrice", "N/A")],
    "Previous Close (USD)": [info.get("regularMarketPreviousClose", "N/A")],
    "Open (USD)": [info.get("regularMarketOpen", "N/A")],
    "Day's Range (USD)": [f"{info.get('regularMarketDayLow', 'N/A')} - {info.get('regularMarketDayHigh', 'N/A')}"],
    "52 Week Range (USD)": [f"{info.get('fiftyTwoWeekLow', 'N/A')} - {info.get('fiftyTwoWeekHigh', 'N/A')}"],
    "Volume": [info.get("regularMarketVolume", "N/A")],
    "Avg. Volume": [info.get("averageDailyVolume3Month", "N/A")],
    "Market Cap (USD)": [info.get("marketCap", "N/A")],
    "Beta (5Y Monthly)": [info.get("beta", "N/A")],
    "PE Ratio (TTM)": [info.get("trailingPE", "N/A")],
    "EPS (TTM)": [info.get("trailingEps", "N/A")],
    "Earnings Date": [info.get("earningsDate", {}).get("maxAge", "N/A")],
    "Forward Dividend & Yield": [f"{info.get('dividendRate', 'N/A')} ({info.get('dividendYield', 'N/A')}%)"],
    "Ex-Dividend Date": [info.get("exDividendDate", "N/A")],
    "1y Target Est (USD)": [info.get("targetMeanPrice", "N/A")]
}

# Convert to DataFrame
df_summary = pd.DataFrame(stock_summary)

# Display the summary
print(df_summary)

# Optionally, save to Excel
df_summary.to_excel("AAPL_Stock_Summary.xlsx", index=False)