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
import sys

if sys.platform == "win32":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

REQUEST_DELAY = 1.5
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
    re.compile(r'(20\d{4})[./-](\d{1,2})[./-](\d{1,2})'),
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

async def perform_Google_Search(session, api_key, query):
    url = f"https://api.scrapingdog.com/google?api_key={api_key}&query={quote(query)}"
    try:
        async with session.get(url, timeout=30, ssl=ssl_context) as resp:
            data = await resp.json()
            return data
    except Exception as e:
        print(f"Error during Google search: {e}")
        return None

def extract_urls(search_data):
    for key in ['organic_results', 'organic_data', 'results', 'items']:
        results = search_data.get(key)
        if results and isinstance(results, list):
            urls = [r.get('link') for r in results if r.get('link')]
            if urls:
                return urls
    return []

async def fetch_with_playwright(page, url):
    try:
        if url.endswith(".pdf"):
            return None
        await page.goto(url, wait_until='domcontentloaded', timeout=90000)
        return await page.content()
    except Exception as e:
        print(f"Playwright failed on {url}: {e}")
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
                print(f"Found ({src}): {kw} | {url} | Date: {date}")
                if len(set(fk for fk, _, _, _, _ in found_entries)) >= MIN_KEYWORDS_PER_COMPANY:
                    return
    await delay(REQUEST_DELAY)

def write_results_to_csv(results_data, output_csv_path, mode='a'):
    output_path = Path(output_csv_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    header = ['Company Name', 'Domain', 'Country', 'Keyword', 'URL', 'Source', 'Date', 'Date Source']
    write_header = mode == 'w' or not output_path.exists()
    with open(output_path, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        for row in results_data:
            writer.writerow(row)
    print(f"Results saved to {output_path}")

async def process_company(company_name, domain, country, all_keywords, session, browser, api_key, progress_callback=print):
    page = await browser.new_page()
    found_entries = []

    def unique_keywords_count():
        return len(set(fk for fk, _, _, _, _ in found_entries))

    all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])

    # ---- Step 1: Own site search ----
    own_query = f'site:{domain} android ({all_kw_query}) (partnership OR collaboration OR customer OR "case study" OR deal)'
    progress_callback(f"Searching own site for {company_name}...")
    search_data = await perform_Google_Search(session, api_key, own_query)
    urls = extract_urls(search_data) if search_data else []

    own_urls = [u for u in urls if not is_third_party(u, domain) and not is_job_link(u)]

    for url in own_urls:
        await process_url(url, page, company_name, domain, all_keywords, found_entries)
        if unique_keywords_count() >= MIN_KEYWORDS_PER_COMPANY:
            break

    # ---- Step 2: Fallback to 3rd party if no own results ----
    if not found_entries:
        progress_callback(f"No own results for {company_name}, searching 3rd-party...")
        third_party_query = (
            f'"{company_name}" ({all_kw_query}) '
            '(partnership OR collaboration OR customer OR "case study" OR deal) -site:{domain}'
        )
        search_data = await perform_Google_Search(session, api_key, third_party_query)
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

async def main(companies_file, keywords_file, scrapingdog_api_key, output_dir="RESULTS_DIR", output_csv_name="scrapingdog_results.csv", progress_callback=print):
    # Create results directory
    results_dir = Path(output_dir)
    results_dir.mkdir(parents=True, exist_ok=True)

    output_csv_path = results_dir / output_csv_name

    companies = []
    all_keywords = []

    # Load companies from CSV or XLSX
    if companies_file.lower().endswith(".csv"):
        import csv as csvlib
        with open(companies_file, 'r', encoding='utf-8') as f:
            reader = csvlib.reader(f)
            for row in reader:
                if len(row) >= 3:
                    company_name, url, country = row[0].strip(), row[1].strip(), row[2].strip()
                    domain = urlparse(url).netloc or url
                    companies.append((company_name, domain, country))
    elif companies_file.lower().endswith((".xls", ".xlsx")):
        import pandas as pd
        df = pd.read_excel(companies_file)
        for _, row in df.iterrows():
            if len(row) >= 3:
                company_name = str(row[0]).strip()
                url = str(row[1]).strip()
                country = str(row[2]).strip()
                domain = urlparse(url).netloc or url
                companies.append((company_name, domain, country))
    else:
        raise ValueError("Companies file must be .csv or .xlsx")

    # Load keywords JSON (fixed here)
    async with aiofiles.open(keywords_file, 'r', encoding='utf-8') as f:
        kws_json = await f.read()
        keywords_data = json.loads(kws_json)
        if isinstance(keywords_data, dict):
            for provider, kws in keywords_data.items():
                for kw in kws:
                    all_keywords.append(kw.lower().strip())
        elif isinstance(keywords_data, list):
            for kw in keywords_data:
                all_keywords.append(kw.lower().strip())
        else:
            raise ValueError("Keywords JSON must be a dict or list")

    # Clear output CSV (write header)
    write_results_to_csv([], output_csv_path, mode='w')

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])
            all_company_results = []
            for idx, (company_name, domain, country) in enumerate(companies, 1):
                progress_callback(f"[{idx}/{len(companies)}] Processing: {company_name} ({country})")
                try:
                    company_results = await process_company(company_name, domain, country, all_keywords, session, browser, scrapingdog_api_key, progress_callback)
                    all_company_results.extend(company_results)
                except Exception as e:
                    progress_callback(f"Error on {company_name}: {e}")
            await browser.close()

            if all_company_results:
                write_results_to_csv(all_company_results, output_csv_path)

    return str(output_csv_path)