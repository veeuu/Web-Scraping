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
import os
from dotenv import load_dotenv
load_dotenv()
# ===== CONFIG =====
COMPANIES_FILE = r'C:\Users\propl\OneDrive\Desktop\work\input.csv'
KEYWORDS_FILE = r'C:\Users\propl\OneDrive\Desktop\work\aws_keywords.json'
RESULTS_DIR = Path(r'C:\Users\propl\OneDrive\Desktop\work\RESULTS_DIR')
OUTPUT_CSV_FILE = RESULTS_DIR / 'company_keyword_results.csv'

SCRAPINGDOG_API_KEY = os.getenv('SCRAPINGDOG_API_KEY')
REQUEST_DELAY = 1.5
MAX_RESULTS_PER_COMPANY = 3
MIN_KEYWORDS_PER_COMPANY = 3  # Min unique keywords per company

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

THIRD_PARTY_KEYWORDS = [
    "partnership", "relationship", "collaboration", "customer", "case study", "deal", "using"
]

JOB_KEYWORDS = ["career", "jobs", "hiring", "recruitment", "apply"]

# aws is compulsory + one of the AWS keywords
SEARCH_QUERY_TEMPLATE = (
    'site:{company_domain} aws ({all_keywords}) '
    '(partnership OR collaboration OR customer OR "case study" OR deal)'
)

# ===== HELPERS =====
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
            print(f"✅ Results saved: {file_path}")
            return data
    except Exception as e:
        print(f"❌ Error searching {domain}: {e}")
        return None

def extract_urls(search_data):
    for key in ['organic_results', 'organic_data', 'results', 'items']:
        results = search_data.get(key)
        if results and isinstance(results, list):
            urls = [r.get('link') for r in results if r.get('link')]
            if urls:
                return urls
    print("⚠️ No organic results found. Keys present:", list(search_data.keys()))
    return []

async def fetch_with_playwright(page, url):
    try:
        if url.endswith(".pdf"):
            return None
        await page.goto(url, wait_until='domcontentloaded', timeout=90000)
        return await page.evaluate("document.body.innerText")
    except Exception as e:
        print(f"❌ Playwright failed on {url}: {e}")
        return None

def is_third_party(url, domain):
    return domain not in urlparse(url).netloc

def is_job_link(url):
    return any(w in url.lower() for w in JOB_KEYWORDS)

def is_relevant_third_party(text, company, kws):
    text = text.lower()
    company_match = re.search(r'\b' + re.escape(company.lower()) + r'\b', text)
    if not company_match:
        return False
    for kw in kws:
        if re.search(r'\b' + re.escape(kw.lower()) + r'\b', text):
            return True
    return False

def is_keyword_present_whole_word(text, keyword):
    return re.search(r'\b' + re.escape(keyword.lower()) + r'\b', text.lower()) is not None

async def process_url(url, page, company_name, domain, all_keywords, found_entries):
    text = await fetch_with_playwright(page, url)
    if not text:
        return

    src = 'own' if not is_third_party(url, domain) else '3rd-party'
    if src == '3rd-party' and not is_relevant_third_party(text, company_name, THIRD_PARTY_KEYWORDS):
        return

    for kw in ["aws"] + all_keywords:
        if is_keyword_present_whole_word(text, kw):
            if kw == "aws" and any(fk == "aws" for fk, _, _ in found_entries):
                continue
            if not any(fk == kw for fk, _, _ in found_entries):
                found_entries.append((kw, url, src))
                print(f"✅ Found ({src}): {kw} | {url}")
                if len(set(fk for fk, _, _ in found_entries)) >= MIN_KEYWORDS_PER_COMPANY:
                    return

    await delay(REQUEST_DELAY)

def write_results_to_csv(results_data, mode='a'):
    OUTPUT_CSV_FILE.parent.mkdir(parents=True, exist_ok=True)
    header = ['Company Name', 'Domain', 'Country', 'Keyword', 'URL', 'Source']

    write_header = mode == 'w' or not OUTPUT_CSV_FILE.exists()
    with open(OUTPUT_CSV_FILE, mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if write_header:
            writer.writerow(header)
        for row in results_data:
            writer.writerow(row)
    print(f"📊 Results saved to {OUTPUT_CSV_FILE}")

async def process_company(company_name, domain, country, all_keywords, session, browser):
    page = await browser.new_page()
    found_entries = []

    def unique_keywords_count():
        return len(set(fk for fk, _, _ in found_entries))

    all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])
    query = SEARCH_QUERY_TEMPLATE.format(company_domain=domain, all_keywords=all_kw_query)

    search_data = await perform_Google_Search(session, company_name, domain, query)
    urls = extract_urls(search_data) if search_data else []

    for url in urls:
        if is_job_link(url):
            continue
        await process_url(url, page, company_name, domain, all_keywords, found_entries)

        # ✅ stop only when we really have 3 different keywords
        if unique_keywords_count() >= MIN_KEYWORDS_PER_COMPANY:
            break

    await page.close()

    results_for_csv = [
        [company_name, domain, country, keyword, url, source]
        for keyword, url, source in found_entries
    ]
    return results_for_csv

# ===== MAIN =====
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
                kw = kw.lower().strip()
                all_keywords.append(kw)

    # Clear CSV before writing
    write_results_to_csv([], mode='w')

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])
            all_company_results = []
            for company_name, domain, country in companies:
                print(f"\n=== Processing: {company_name} ({country}) ===")
                try:
                    company_specific_results = await process_company(
                        company_name, domain, country, all_keywords, session, browser
                    )
                    all_company_results.extend(company_specific_results)
                except Exception as e:
                    print(f"❌ Error on {company_name}: {e}")
            await browser.close()
            if all_company_results:
                write_results_to_csv(all_company_results)

if __name__ == "__main__":
    asyncio.run(main())
