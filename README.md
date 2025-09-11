<img width="1918" height="973" alt="readme" src="https://github.com/user-attachments/assets/00be50b3-81f3-4356-8510-b2bb74225248" />
<img width="1917" height="1043" alt="scrapingdog" src="https://github.com/user-attachments/assets/6e6678d7-904f-4e51-ac8e-5608b18bf416" />

# Web Scraping and Analysis Project

## Overview

This project encompasses a collection of Python scripts and related files designed for web scraping, data extraction, and analysis of company information, particularly focusing on financial data, executive profiles, and technology usage. The project leverages various libraries such as `BeautifulSoup`, `Selenium`, `Playwright`, `yfinance`, `pandas`, and others to automate the process of gathering and analyzing data from the web.

## Project Structure

The project is organized into several directories and files, each serving a specific purpose:

### Core Scripts

-   **`dash.py`**: A Streamlit dashboard application that provides a user interface for uploading data, initiating web scraping and analysis, and downloading results.
-   **`try.py`**: Main script for web scraping, content extraction, semantic analysis, and relevance detection.
-   **`backend.py`**: Contains backend functions for web scraping, data extraction, and relevance scoring.
-   **`aboutus.py`**: Script for extracting executive information (names and designations) from company "About Us" pages.
-   **`finance.py`**: Script for scraping financial data (company profile, key executives) from Yahoo Finance.
-   **`news.py`**: Script for scraping news links related to specific tickers from Yahoo Finance.
-   **`sustanibility.py`**: Script for crawling websites and searching for specific keywords related to sustainability.

### Financial Data Scripts

-   **`analysis.py`**: Scrapes analysis data from Yahoo Finance using Selenium.
-   **`financial.py`**: Extracts financial statements (income, balance sheet, cash flow) from Yahoo Finance using `yfinance`.
-   **`profile.py`**: Extracts company profile information and key executives from Yahoo Finance using `yfinance`.
-   **`summary.py`**: Generates a stock summary from Yahoo Finance using `yfinance`.
-   **`complete.py`**: Combines the functionality of `analysis.py`, `profile.py`, `financial.py`, and `summary.py` to extract and consolidate all data into a single Excel file.

### Utility Scripts

-   **`csvtojson.py`**: Converts a CSV file to a JSON file.
-   **`testtechno.py`**: Web scraping script to identify technology usage by companies.
-   **`testtechnodate.py`**: Enhanced version of `testtechno.py` that also extracts dates from web pages.
-   **`voice.py`**: Crawls websites to identify voice and CCaaS (Contact Center as a Service) providers.
-   **`revenue.py`**: Searches Google for company revenue information and extracts it from web pages.

### Configuration Files

-   **`.env`**: Contains API keys and other environment-specific settings.
-   **`vmware_keywords (1).json`**: JSON file containing a list of VMware-related keywords.
-   **`aws_keywords.json`**: JSON file containing a list of AWS-related keywords.

### Data Files

-   **`input (3).csv`**: CSV file containing a list of companies, domains, and countries.
-   **`Pathos Communication Account List.csv`**: CSV file used by `aboutus.py` to extract executive information.
-   **`yahoo_news_links.csv`**: CSV file containing Yahoo Finance news links.
-   **`yahoo_news_links_with_titles.csv`**: CSV file containing Yahoo Finance news links with titles.
-   **`yahoo_news_links_with_titles_dates.csv`**: CSV file for storing Yahoo Finance news links with titles and dates.
-   **`AU_Supplemental VMware list_250908.xlsx - Input_automation.csv`**: CSV file containing a list of Australian companies and their domains.
-   **`companies.csv`**: CSV file used by `sustanibility1.py` to crawl company websites.
-   **`companies1.csv`**: CSV file used by `voice.py` to crawl company websites.
-   **`revenue_sheet.csv`**: CSV file used by `revenue.py` to search for company revenue information.

### Workflow Directory

-   **`workflow/`**: Contains a React application (`my-app/`) and a JavaScript file (`finance.js`) for web scraping.

### Results Directory

-   **`RESULTS_DIR/`**: Directory for storing output files, including JSON files containing search results and CSV files containing extracted data.

## Dependencies

The project relies on the following Python libraries:

-   `aiofiles`
-   `aiohttp`
-   `asyncio`
-   `bs4` (BeautifulSoup4)
-   `csv`
-   `dotenv`
-   `httpx`
-   `json`
-   `logging`
-   `nest_asyncio`
-   `nltk`
-   `os`
-   `pandas`
-   `pathlib`
-   `platform`
-   `pprint`
-   `re`
-   `requests`
-   `selenium`
-   `sentence_transformers`
-   `sklearn`
-   `spacy`
-   `streamlit`
-   `trafilatura`
-   `urllib3`
-   `yfinance`

To install the necessary dependencies, run:

```bash
pip install aiofiles aiohttp beautifulsoup4 csv python-dotenv httpx json nltk pandas playwright re requests selenium sentence-transformers scikit-learn spacy streamlit trafilatura urllib3 yfinance
