import asyncio
import csv
import json
import re
import ssl
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, urlparse
from datetime import datetime
import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
from playwright.async_api import async_playwright
import os
from dotenv import load_dotenv

load_dotenv()
# === Config ===
COMPANIES_FILE = r'C:\Users\propl\OneDrive\Desktop\work\input.csv'
KEYWORDS_FILE = r'C:\Users\propl\OneDrive\Desktop\work\aws_keywords.json'
RESULTS_DIR = Path(r'C:\Users\propl\OneDrive\Desktop\work\RESULTS_DIR')
RESULTS_CSV = r'C:\Users\propl\OneDrive\Desktop\work\test.csv'


REQUEST_DELAY = 1.5
SCRAPINGDOG_API_KEY = os.getenv('SCRAPINGDOG_API_KEY')
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

NOW = datetime.now()
CURRENT_YEAR, CURRENT_MONTH = NOW.year, NOW.month

MONTH_MAP = {m: i+1 for i, m in enumerate([
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'])}
MONTH_REGEX = '|'.join(MONTH_MAP.keys())
DATE_PATTERNS = [
    re.compile(rf'({MONTH_REGEX})\s+(\d{{1,2}}),?\s*(20\d{{2}})'),
    re.compile(rf'({MONTH_REGEX})\s*(20\d{{2}})'),
    re.compile(r'(\d{1,2})[-/](\d{1,2})[-/](20\d{{2}})'),
    re.compile(r'(20\d{{2}})[./-](\d{{1,2}})[./-](\d{{1,2}})'),
    re.compile(r'\b(19\d{{2}}|20\d{{2}})\b')
]

THIRD_PARTY_KEYWORDS = [
    "partnership", "relationship", "collaboration", "customer", "case study", "deal", "using"
]

JOB_KEYWORDS = ["career", "jobs", "hiring", "recruitment", "apply"]

SEARCH_QUERY_TEMPLATE = (
  '"{company_name}" ({all_keywords}) (partnership OR collaboration OR customer OR "case study" OR deal) -site:{company_domain}'
)


def sanitize_filename(name): return re.sub(r'[\\/:*?"<>|]', '_', name)

def parse_date(text):
    text = text.lower()
    m = re.search(r'd:(\d{4})(\d{2})', text)
    if m: return f"{int(m.group(2)):02d} {m.group(1)}"
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            g = match.groups()
            if len(g) >= 2 and g[0] in MONTH_MAP:
                return f"{MONTH_MAP[g[0]]:02d} {g[-1]}"
            if len(g) == 3:
                return f"{int(g[1]):02d} {g[2]}"
            if len(g) == 1:
                return f"{CURRENT_MONTH:02d} {g[0]}"
    return None

async def ensure_directory_exists(path: Path):
    path.mkdir(parents=True, exist_ok=True)

async def delay(seconds): await asyncio.sleep(seconds)

async def perform_google_search(session, company_name, domain, all_kw_query):
    query = SEARCH_QUERY_TEMPLATE.format(
        company_name=company_name, company_domain=domain, all_keywords=all_kw_query)
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
            if urls: return urls
    print("⚠️ No organic results found. Keys present:", list(search_data.keys()))
    return []

async def fetch_with_playwright(page, url):
    try:
        if url.endswith(".pdf"): return None
        await page.goto(url, wait_until='networkidle', timeout=45000)
        return await page.evaluate("document.body.innerText")
    except Exception as e:
        print(f"❌ Playwright failed on {url}: {e}")
        return None

async def fetch_page_content(session, url):
    try:
        async with session.get(url, timeout=20, ssl=ssl_context) as resp:
            ctype = resp.headers.get('content-type', '')
            data = await resp.read()
            if 'pdf' in ctype: return data, "pdf"
            if 'html' in ctype or 'text' in ctype: return data, "html"
            return data, "other"
    except Exception as e:
        print(f"❌ Failed to fetch {url}: {e}")
        return None, "load_failed"

async def get_date(url, session):
    content, ctype = await fetch_page_content(session, url)
    if not content: return None
    if ctype == "pdf":
        try:
            pdf = PdfReader(BytesIO(content))
            for k, v in (pdf.metadata or {}).items():
                d = parse_date(str(v))
                if d: return d
            for page in pdf.pages:
                d = parse_date(page.extract_text() or '')
                if d: return d
        except: return None
    if ctype == "html":
        soup = BeautifulSoup(content, 'html.parser')
        for tag in soup.find_all(['time','meta','span','div','title','h1','h2','h3']):
            d = parse_date(tag.get_text() or tag.get('content',''))
            if d: return d
        d = parse_date(soup.get_text(" ", strip=True))
        if d: return d
    m = re.search(r'(20\d{2}|19\d{2})', url)
    if m: return f"{CURRENT_MONTH:02d} {m.group(1)}"
    return None

def is_third_party(url, domain): return domain not in urlparse(url).netloc
def is_job_link(url): return any(w in url.lower() for w in JOB_KEYWORDS)
def is_relevant_third_party(text, company, kws):
    text = text.lower()
    return company.lower() in text and any(k in text for k in kws)

def analyze_found(found_entries):
    previous, latest = None, None
    for e in found_entries:
        _, _, _, year, _ = e
        if year < CURRENT_YEAR:
            if not previous or previous[3] < year: previous = e
        else:
            if not latest or latest[3] < year: latest = e
    return previous, latest

async def crawl_with_playwright(domain, all_keywords, page, session, found_entries):
    visited, queue = set(), [f"https://{domain}/"]
    while queue and len(found_entries) < 2:
        url = queue.pop(0)
        if url in visited: continue
        visited.add(url)
        try:
            await page.goto(url, wait_until='networkidle', timeout=45000)
            text = await page.evaluate("document.body.innerText") or ""
            html = await page.content()
            soup = BeautifulSoup(html, 'html.parser')
            for a in soup.find_all("a", href=True):
                href = a['href']
                if href.startswith('/'): href = f"https://{domain}{href}"
                elif not href.startswith(f"https://{domain}"): continue
                if href not in visited: queue.append(href)
            for kw in all_keywords:
                if any(fk == kw for fk, *_ in found_entries): continue
                if kw in text.lower():
                    date_str = await get_date(url, session) or '-'
                    year = int(date_str.split()[1]) if date_str != '-' else 0
                    found_entries.append((kw, url, date_str, year, 'own-crawl'))
                    print(f"✅ Found by crawl: {kw} | {url} | {date_str}")
            await delay(REQUEST_DELAY)
        except Exception as e:
            print(f"❌ Crawl failed on {url}: {e}")

async def process_company(company_name, domain, country, all_keywords, keyword_to_provider, session, browser, writer):
    page = await browser.new_page()
    found_entries = []
    all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])
    search_data = await perform_google_search(session, company_name, domain, all_kw_query)
    if not search_data: 
        await page.close()
        return

    urls = extract_urls(search_data)
    if not urls:
        print(f"🔄 Retrying search for {company_name}")
        await delay(REQUEST_DELAY)
        search_data = await perform_google_search(session, company_name, domain, all_kw_query)
        urls = extract_urls(search_data)
        if not urls: 
            print(f"❌ Still no results for {company_name}")
            await page.close()
            return

    urls_third_party = [u for u in urls if is_third_party(u, domain)]
    urls_own = [u for u in urls if not is_third_party(u, domain)]
    urls_ordered = urls_third_party + urls_own

    async def process_url(url):
        if is_job_link(url): return
        text = await fetch_with_playwright(page, url)
        if not text: return
        src = '3rd-party' if is_third_party(url, domain) else 'own'
        if src == '3rd-party' and not is_relevant_third_party(text, company_name, THIRD_PARTY_KEYWORDS): return
        for kw in all_keywords:
            if any(fk == kw and furl == url for fk, furl, *_ in found_entries): continue
            if kw in text.lower():
                date_str = await get_date(url, session) or '-'
                year = int(date_str.split()[1]) if date_str != '-' else 0
                found_entries.append((kw, url, date_str, year, src))
                print(f"✅ Found ({src}): {kw} | {url} | {date_str}")

    for url in urls_ordered:
        await process_url(url)
        if len(found_entries) >= 2: break
        await delay(REQUEST_DELAY)

    if not any(e[-1] == '3rd-party' for e in found_entries):
        print(f"🔄 No relevant 3rd-party results. Crawling site for {company_name}")
        await crawl_with_playwright(domain, all_keywords, page, session, found_entries)

    await page.close()
    third_party_entries = [e for e in found_entries if e[-1] == '3rd-party']
    selected_entries = third_party_entries if third_party_entries else found_entries

    prev, lat = analyze_found(selected_entries)

    writer.writerow([
        company_name, domain, country,
        prev[0] if prev else '-', prev[1] if prev else '-', prev[2] if prev else '-',
        lat[0] if lat else '-', lat[1] if lat else '-', lat[2] if lat else '-'
    ])

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

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])
            with open(RESULTS_CSV, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.writer(csvfile)
                writer.writerow(['company', 'URL', 'country', 'keyword of previous', 'previous link', 'date of previous link', 'latest keyword', 'latest link', 'latest date'])
                for company_name, domain, country in companies:
                    print(f"=== Processing: {company_name} ({country}) ===")
                    try:
                        await process_company(company_name, domain, country, all_keywords, keyword_to_provider, session, browser, writer)
                    except Exception as e:
                        print(f"❌ Error on {company_name}: {e}")
            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
