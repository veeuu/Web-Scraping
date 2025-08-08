import asyncio
import json
import re
import ssl
from pathlib import Path
from urllib.parse import quote, urlparse
import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from playwright.async_api import async_playwright
import csv
from datetime import datetime
import urllib3
import os
from dotenv import load_dotenv
load_dotenv()
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
COMPANIES_FILE = r'C:\Users\propl\OneDrive\Desktop\work\input.csv'
KEYWORDS_FILE = r'C:\Users\propl\OneDrive\Desktop\work\os_keywords.json'
RESULTS_DIR = Path(r'C:\Users\propl\OneDrive\Desktop\work\RESULTS_DIR')
OUTPUT_CSV_FILE = RESULTS_DIR / 'company_keyword_results_with_dates(OS).csv'

SCRAPINGDOG_API_KEY = os.getenv('SCRAPINGDOG_API_KEY')
REQUEST_DELAY = 1.5
MAX_RESULTS_PER_COMPANY = 3
MIN_KEYWORDS_PER_COMPANY = 3

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

NOW = datetime.now()
CURRENT_YEAR, CURRENT_MONTH = NOW.year, NOW.month

MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}
MONTH_REGEX = '|'.join(MONTH_MAP)

DATE_PATTERNS = [
    re.compile(rf'({MONTH_REGEX})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s*(20\d{{2}})', re.I),
    re.compile(rf'({MONTH_REGEX})\s*(20\d{{2}})', re.I),
    re.compile(r'(\d{1,2})[-/](\d{1,2})[-/](20\d{2})'),
    re.compile(r'(20\d{2})[./-](\d{1,2})[./-](\d{1,2})'),
    re.compile(r'\b(19\d{2}|20\d{2})\b')
]

def parse_date(text):
    text = text.lower()
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            g = match.groups()
            try:
                if len(g) == 3:
                    if g[0] in MONTH_MAP:
                        m = MONTH_MAP[g[0]]
                        return f"{m:02d} {g[2]}"
                    else:
                        return f"{int(g[1]):02d} {g[2]}"
                elif len(g) == 2 and g[0] in MONTH_MAP:
                    m = MONTH_MAP[g[0]]
                    return f"{m:02d} {g[1]}"
                elif len(g) == 1:
                    y = int(g[0])
                    if 1900 <= y <= CURRENT_YEAR:
                        return f"{CURRENT_MONTH:02d} {y}"
            except Exception:
                continue
    return None

async def fetch_date_from_html(content):
    soup = BeautifulSoup(content, 'html.parser')

    for selector in [
        '.local-date', '.pr-date', 'time',
        'meta[name="pubdate"]',
        'meta[property="article:published_time"]'
    ]:
        el = soup.select_one(selector)
        if el:
            date_text = el.get_text() if el.name != 'meta' else el.get('content', '')
            if (d := parse_date(date_text)):
                return d, selector

 
    for tag in ['title', 'h1', 'h2', 'h3']:
        for el in soup.find_all(tag):
            if (d := parse_date(el.get_text())):
                return d, tag


    footer_text = ""
    for footer in soup.find_all(['footer']):
        footer_text += " " + footer.get_text(" ", strip=True)


    copyright_patterns = [
        re.compile(r'©\s*(19\d{2}|20\d{2})', re.I),
        re.compile(r'copyright\s*(19\d{2}|20\d{2})', re.I),
        re.compile(r'(19\d{2}|20\d{2})\s*[-–]\s*(19\d{2}|20\d{2})') 
    ]

    for pattern in copyright_patterns:
        matches = pattern.findall(footer_text)
        if matches:
           
            years = []
            for m in matches:
                if isinstance(m, tuple):
                    years.extend(m)
                else:
                    years.append(m)
            years = [int(y) for y in years if y.isdigit()]
            if years:
                latest_year = max(years)  
                return f"{CURRENT_MONTH:02d} {latest_year}", "copyright/footer"


    
    text = soup.get_text(" ", strip=True)
    if (d := parse_date(text)):
        return d, "body_text"

    return None, "not_found"


def sanitize_filename(name):
    return re.sub(r'[\\/:*?"<>|]', '_', name)

async def ensure_directory_exists(path: Path):
    path.mkdir(parents=True, exist_ok=True)

async def delay(seconds):
    await asyncio.sleep(seconds)

async def perform_Google_Search(session, company_name, domain, query):
    url = f"https://api.scrapingdog.com/google?api_key={SCRAPINGDOG_API_KEY}&query={quote(query)}"
    print(f"🔍 Searching: '{query}'")
    try:
        async with session.get(url, timeout=30, ssl=ssl_context) as resp:
            data = await resp.json()
            file_path = RESULTS_DIR / f"{sanitize_filename(domain)}.json"
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(json.dumps(data, indent=2))
            print(f" Results saved: {file_path}")
            return data
    except Exception as e:
        print(f"Error searching {domain}: {e}")
        return None

def extract_urls(search_data):
    for key in ['organic_results', 'organic_data', 'results', 'items']:
        results = search_data.get(key)
        if results and isinstance(results, list):
            urls = [r.get('link') for r in results if r.get('link')]
            if urls:
                return urls
    print(" No organic results found.")
    return []

async def fetch_with_playwright(page, url):
    try:
        if url.endswith(".pdf"):
            return None
        await page.goto(url, wait_until='domcontentloaded', timeout=90000)
        return await page.content()
    except Exception as e:
        print(f" Playwright failed on {url}: {e}")
        return None

def is_job_link(url):
    JOB_KEYWORDS = ["career", "jobs", "hiring", "recruitment", "apply"]
    return any(w in url.lower() for w in JOB_KEYWORDS)

def is_third_party(url, domain):
    return domain not in urlparse(url).netloc

async def process_url(url, page, company_name, domain, all_keywords, found_entries):
    content = await fetch_with_playwright(page, url)
    if not content:
        return

    src = 'own' if domain in urlparse(url).netloc else '3rd-party'
    text = BeautifulSoup(content, 'html.parser').get_text(" ", strip=True)

    date, date_src = await fetch_date_from_html(content)

    for kw in ["android"] + all_keywords:
        if re.search(r'\b' + re.escape(kw.lower()) + r'\b', text.lower()):
            if kw == "android" and any(fk == "android" for fk, _, _, _, _ in found_entries):
                continue
            if not any(fk == kw for fk, _, _, _, _ in found_entries):
                found_entries.append((kw, url, src, date or '', date_src))
                print(f" Found ({src}): {kw} | {url} | Date: {date}")
                if len(set(fk for fk, _, _, _, _ in found_entries)) >= MIN_KEYWORDS_PER_COMPANY:
                    return
    await delay(REQUEST_DELAY)

def write_results_to_csv(results_data, mode='a'):
    OUTPUT_CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    header = ['Company Name', 'Domain', 'Country', 'Keyword', 'URL', 'Source', 'Date', 'Date Source']
    write_header = mode == 'w' or not OUTPUT_CSV_FILE.exists()
    with open(OUTPUT_CSV_FILE, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        for row in results_data:
            writer.writerow(row)
    print(f"Results saved to {OUTPUT_CSV_FILE}")

async def process_company(company_name, domain, country, all_keywords, session, browser):
    page = await browser.new_page()
    found_entries = []

    def unique_keywords_count():
        return len(set(fk for fk, _, _, _, _ in found_entries))

    all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])

    # ---- Step 1: Own site search ----
    own_query = f'site:{domain} android ({all_kw_query}) (partnership OR collaboration OR customer OR "case study" OR deal)'
    search_data = await perform_Google_Search(session, company_name, domain, own_query)
    urls = extract_urls(search_data) if search_data else []

    own_urls = [u for u in urls if not is_third_party(u, domain) and not is_job_link(u)]

    for url in own_urls:
        await process_url(url, page, company_name, domain, all_keywords, found_entries)
        if unique_keywords_count() >= MIN_KEYWORDS_PER_COMPANY:
            break

    # ---- Step 2: Fallback to 3rd party if no own results ----
    if not found_entries:
        print(f"⚠️ No own results for {company_name}, searching 3rd-party...")
        third_party_query = (
            f'"{company_name}" ({all_kw_query}) '
            '(partnership OR collaboration OR customer OR "case study" OR deal) -site:{domain}'
        )
        search_data = await perform_Google_Search(session, company_name, domain, third_party_query)
        urls = extract_urls(search_data) if search_data else []

        third_party_urls = [u for u in urls if is_third_party(u, domain) and not is_job_link(u)]

        for url in third_party_urls:
            await process_url(url, page, company_name, domain, all_keywords, found_entries)
            if unique_keywords_count() >= MIN_KEYWORDS_PER_COMPANY:
                break

    await page.close()

    results_for_csv = [
        [company_name, domain, country, keyword, url, source, date, date_src]
        for keyword, url, source, date, date_src in found_entries
    ]
    return results_for_csv

async def main():
    await ensure_directory_exists(RESULTS_DIR)
    companies, all_keywords = [], []

    # Load companies
    async with aiofiles.open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        async for line in f:
            parts = re.split(r'[,|\t]', line.strip())
            if len(parts) >= 3:
                company_name, url, country = parts[0].strip(), parts[1].strip(), parts[2].strip()
                domain = urlparse(url).netloc or url
                companies.append((company_name, domain, country))

    # Load AWS keywords
    async with aiofiles.open(KEYWORDS_FILE, 'r') as f:
        for provider, kws in (json.loads(await f.read())).items():
            for kw in kws:
                all_keywords.append(kw.lower().strip())

    # Clear CSV
    write_results_to_csv([], mode='w')

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])
            all_company_results = []
            for company_name, domain, country in companies:
                print(f"\n=== Processing: {company_name} ({country}) ===")
                try:
                    company_results = await process_company(company_name, domain, country, all_keywords, session, browser)
                    all_company_results.extend(company_results)
                except Exception as e:
                    print(f"❌ Error on {company_name}: {e}")
            await browser.close()
            if all_company_results:
                write_results_to_csv(all_company_results)

if __name__ == "__main__":
    asyncio.run(main())
