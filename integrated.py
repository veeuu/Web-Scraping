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
COMPANIES_FILE = r'C:\Users\propl\OneDrive\Desktop\work\input.csv'
KEYWORDS_FILE = r'C:\Users\propl\OneDrive\Desktop\work\aws_keywords.json'
RESULTS_DIR = Path(r'C:\Users\propl\OneDrive\Desktop\work\RESULTS_DIR')
OUTPUT_CSV_FILE = RESULTS_DIR / 'company_keyword_results.csv' 

SCRAPINGDOG_API_KEY = os.getenv('SCRAPINGDOG_API_KEY')
REQUEST_DELAY = 1.5
MAX_RESULTS_PER_COMPANY = 3

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

THIRD_PARTY_KEYWORDS = [
    "partnership", "relationship", "collaboration", "customer", "case study", "deal", "using"
]

JOB_KEYWORDS = ["career", "jobs", "hiring", "recruitment", "apply"]

# 🔎 First attempt: very focused single-keyword search
SEARCH_QUERY_TEMPLATE_SINGLE = 'site:{company_domain} {keyword}'

# 🔎 Fallback: broader all-keywords search
SEARCH_QUERY_TEMPLATE_ALL = (
    'site:{company_domain} ({all_keywords}) '
    '(partnership OR collaboration OR customer OR "case study" OR deal)'
)


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

async def crawl_with_playwright(domain, all_keywords, page, session, found_entries):
    visited, queue = set(), [f"https://{domain}/"]
    print(f"Starting crawl for {domain}")

    while queue and len(found_entries) < MAX_RESULTS_PER_COMPANY:
        url = queue.pop(0)
        if url in visited:
            continue
        visited.add(url)
        try:
            await page.goto(url, wait_until='domcontentloaded', timeout=90000)
            text = await page.evaluate("document.body.innerText") or ""
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')

            for kw in all_keywords:
                if len(found_entries) >= MAX_RESULTS_PER_COMPANY:
                    break
                if is_keyword_present_whole_word(text, kw):
                    if not any(fk == kw and furl == url for fk, furl, *_ in found_entries):
                        found_entries.append((kw, url, 'own-crawl'))
                        print(f"✅ Found by crawl: {kw} | {url}")

            for a in soup.find_all("a", href=True):
                if len(found_entries) >= MAX_RESULTS_PER_COMPANY:
                    break
                href = a['href']
                parsed_href = urlparse(href)
                if parsed_href.scheme and parsed_href.netloc:
                    absolute_href = href
                elif href.startswith('/'):
                    absolute_href = f"https://{domain}{href}"
                else:
                    continue
                if absolute_href.startswith(f"https://{domain}") and absolute_href not in visited:
                    queue.append(absolute_href)

            await delay(REQUEST_DELAY)
        except Exception as e:
            print(f"❌ Crawl failed on {url}: {e}")

async def write_results_to_csv(results_data):
    await ensure_directory_exists(RESULTS_DIR)
    header = ['Company Name', 'Domain', 'Country', 'Keyword', 'URL', 'Source']
    mode = 'w' if not OUTPUT_CSV_FILE.exists() else 'a'

    async with aiofiles.open(OUTPUT_CSV_FILE, mode=mode, newline='', encoding='utf-8') as f:
        writer = csv.writer(f)
        if mode == 'w':
            await writer.writerow(header)
        for row in results_data:
            await writer.writerow(row)
    print(f"📊 Results saved to {OUTPUT_CSV_FILE}")

async def process_company(company_name, domain, country, all_keywords, keyword_to_provider, session, browser):
    page = await browser.new_page()
    found_entries = []

    async def process_url(url):
        if len(found_entries) >= MAX_RESULTS_PER_COMPANY:
            return
        text = await fetch_with_playwright(page, url)
        if not text:
            return
        src = 'own' if not is_third_party(url, domain) else '3rd-party'
        if src == '3rd-party' and not is_relevant_third_party(text, company_name, THIRD_PARTY_KEYWORDS):
            return
        for kw in all_keywords:
            if is_keyword_present_whole_word(text, kw):
                if not any(fk == kw and furl == url for fk, furl, *_ in found_entries):
                    found_entries.append((kw, url, src))
                    print(f"✅ Found ({src}): {kw} | {url}")
                    break  # stop after 1 keyword match per URL

    # --- Step 1: Single keyword (aws) ---
    primary_kw = "aws"
    query = SEARCH_QUERY_TEMPLATE_SINGLE.format(company_domain=domain, keyword=primary_kw)
    single_data = await perform_Google_Search(session, company_name, domain, query)
    urls = extract_urls(single_data)[:MAX_RESULTS_PER_COMPANY] if single_data else []

    for url in urls:
        await process_url(url)
        if len(found_entries) >= MAX_RESULTS_PER_COMPANY:
            break

    # --- Step 2: All keywords if less than 3 results ---
    if len(found_entries) < MAX_RESULTS_PER_COMPANY:
        all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])
        query = SEARCH_QUERY_TEMPLATE_ALL.format(company_domain=domain, all_keywords=all_kw_query)
        all_data = await perform_Google_Search(session, company_name, domain, query)
        urls = extract_urls(all_data)[:MAX_RESULTS_PER_COMPANY] if all_data else []

        for url in urls:
            await process_url(url)
            if len(found_entries) >= MAX_RESULTS_PER_COMPANY:
                break

    await page.close()

    results_for_csv = [
        [company_name, domain, country, keyword, url, source]
        for keyword, url, source in found_entries[:MAX_RESULTS_PER_COMPANY]
    ]

    return results_for_csv



async def main():
    await ensure_directory_exists(RESULTS_DIR)
    companies, all_keywords, keyword_to_provider = [], [], {}

    async with aiofiles.open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        async for line in f:
            parts = re.split(r'[,|\t]', line.strip())
            if len(parts) >= 3:
                company_name, url, country = parts[0].strip(), parts[1].strip(), parts[2].strip()
                domain = urlparse(url).netloc or url
                companies.append((company_name, domain, country))

    async with aiofiles.open(KEYWORDS_FILE, 'r') as f:
        for provider, kws in (json.loads(await f.read())).items():
            for kw in kws:
                kw = kw.lower().strip()
                all_keywords.append(kw)
                keyword_to_provider[kw] = provider

    await write_results_to_csv([])

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])
            all_company_results = [] 
            for company_name, domain, country in companies:
                print(f"\n=== Processing: {company_name} ({country}) ===")
                try:
                    company_specific_results = await process_company(
                        company_name, domain, country, all_keywords, keyword_to_provider, session, browser
                    )
                    all_company_results.extend(company_specific_results) 
                except Exception as e:
                    print(f"❌ Error on {company_name}: {e}")
            await browser.close()
            if all_company_results:
                await write_results_to_csv(all_company_results)

if __name__ == "__main__":
    asyncio.run(main())