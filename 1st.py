import asyncio
import json
import re
import ssl
from io import BytesIO
from pathlib import Path
from tempfile import NamedTemporaryFile
from urllib.parse import quote, urlparse
from datetime import datetime
import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from docx import Document
import pikepdf
from PyPDF2 import PdfReader
from pdfminer.high_level import extract_text as pdfminer_extract_text
from playwright.async_api import async_playwright
import os
from dotenv import load_dotenv

load_dotenv()

COMPANIES_FILE = r'C:\Users\propl\OneDrive\Desktop\work\input.csv'
KEYWORDS_FILE = r'C:\Users\propl\OneDrive\Desktop\work\aws_keywords.json'
RESULTS_DIR = Path(r'C:\Users\propl\OneDrive\Desktop\work\RESULTS_DIR')

SCRAPINGDOG_API_KEY = os.getenv('SCRAPINGDOG_API_KEY')
REQUEST_DELAY = 1.5
MAX_KEYWORDS_PER_COMPANY = 3

SEARCH_QUERY_TEMPLATE = (
  'site:{company_domain} ({all_keywords}) (partnership OR collaboration OR customer OR "case study" OR deal)'
)

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
    re.compile(rf'({MONTH_REGEX})\s+(\d{{1,2}}),\s*(20\d{{2}})'),
    re.compile(rf'({MONTH_REGEX})\s*(20\d{{2}})'),
    re.compile(r'(\d{1,2})[-/](\d{1,2})[-/](20\d{{2}})'),
    re.compile(r'(20\d{{2}})[./-](\d{{1,2}})[./-](\d{{1,2}})'),
    re.compile(r'(\d{1,2})[-/](20\d{{2}})'),
    re.compile(r'\b(19\d{{2}}|20\d{{2}})\b')
]

def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', '_', name)

def parse_date(text):
    text = text.lower()
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            g = match.groups()
            if len(g) == 3 and ',' in match.group(0):
                m = MONTH_MAP.get(g[0], 12)
                return f"{m:02d} {g[2]}"
            if len(g) == 2 and g[0] in MONTH_MAP:
                m = MONTH_MAP.get(g[0], 12)
                return f"{m:02d} {g[1]}"
            if len(g) == 3:
                return f"{int(g[1]):02d} {g[2]}"
            if len(g) == 1:
                y = int(g[0])
                if y <= CURRENT_YEAR:
                    return f"{CURRENT_MONTH:02d} {y}"
    return None

async def ensure_directory_exists(path: Path):
    path.mkdir(parents=True, exist_ok=True)

async def delay(seconds):
    await asyncio.sleep(seconds)

async def perform_google_search(session, domain, all_kw_query):
    query = SEARCH_QUERY_TEMPLATE.format(
        company_domain=domain, all_keywords=all_kw_query
    )
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
        print(f" Error searching {domain}: {e}")
        return None

def extract_urls(search_data, domain):
    results = search_data.get('organic_results') or search_data.get('organic_data')
    if not results:
        print(" No organic results.")
        return []
    urls = [r.get('link') for r in results if r.get('link')]
    urls = [u for u in urls if domain in u]
    return urls

async def fetch_page_content(session, url):
    try:
        async with session.get(url, timeout=20, ssl=ssl_context) as resp:
            content_type = resp.headers.get('content-type', '')
            data = await resp.read()
            if 'pdf' in content_type:
                return data, "pdf"
            if 'html' in content_type or 'text' in content_type:
                return data, "html"
            return data, "other"
    except Exception as e:
        print(f"❌ Failed to fetch {url}: {e}")
        return None, "load_failed"

async def get_date(url, session):
    content, ctype = await fetch_page_content(session, url)

    if ctype in ("load_failed", "file_not_found") or content is None:
        return None, ctype

    if ctype == "pdf":
        try:
            pdf = PdfReader(BytesIO(content))
            info = pdf.metadata
            for attr in ['/ModDate', '/CreationDate']:
                if attr in info and info[attr]:
                    if (d := parse_date(info[attr])):
                        return d, "pdf_metadata"
            for page in pdf.pages:
                txt = page.extract_text() or ''
                if (d := parse_date(txt)):
                    return d, "pdf_text"
        except Exception as e:
            print(f"❌ PDF error: {e}")
        return None, "pdf_no_date"

    if ctype == "html":
        soup = BeautifulSoup(content, 'html.parser')

        for selector, label in [
            ('.local-date', ".local-date"),
            ('.pr-date', ".pr-date")
        ]:
            el = soup.select_one(selector)
            if el and (d := parse_date(el.get_text())):
                return d, label

        for tag in ['time', 'span', 'title', 'h1', 'h2', 'h3']:
            for el in soup.find_all(tag):
                if (d := parse_date(el.get_text())):
                    return d, tag

        if (d := parse_date(url)):
            return d, "url"

        copyright_patterns = [
            re.compile(r'©\s*(19\d{2}|20\d{2})'),
            re.compile(r'copyright\s+©?\s*(19\d{2}|20\d{2})', re.IGNORECASE)
        ]
        text = soup.get_text(" ", strip=True)
        for pattern in copyright_patterns:
            match = pattern.search(text)
            if match:
                year = match.group(1)
                return f"12 {year}", "copyright"

        body_text = soup.get_text(" ", strip=True)
        if (d := parse_date(body_text)):
            return d, "body_text"

    return None, "not_found"

async def fetch_with_playwright(page, url):
    try:
        if url.endswith(".pdf"):
            return None
        await page.goto(url, wait_until='networkidle', timeout=45000)
        text = await page.evaluate("document.body.innerText")
        return text
    except Exception as e:
        print(f"❌ Playwright failed on {url}: {e}")
        return None

async def process_company(domain, country, all_keywords, keyword_to_provider, session, browser):
    page = await browser.new_page()
    found_keywords = set()
    found_entries = []

    all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])
    search_data = await perform_google_search(session, domain, all_kw_query)
    if not search_data:
        print(f" No search data for {domain}")
        await page.close()
        return

    urls = extract_urls(search_data, domain)
    if not urls:
        print(f" No relevant URLs found for {domain}")
        await page.close()
        return

    for url in urls:
        if len(found_entries) >= MAX_KEYWORDS_PER_COMPANY:
            break
        if any(x in url.lower() for x in ["career", "jobs", "hiring", "recruitment", "apply"]):
            continue

        text = await fetch_with_playwright(page, url)
        if not text:
            continue

        lower_text = text.lower()
        for kw in all_keywords:
            if kw in found_keywords:
                continue
            if re.search(rf'\b{re.escape(kw)}\b', lower_text, re.IGNORECASE):
                date_str, date_source = await get_date(url, session)
                print(f" Found: keyword='{kw}' | provider='{keyword_to_provider[kw]}' | url='{url}' | date='{date_str or '-'}' ({date_source})")
                found_keywords.add(kw)
                found_entries.append((kw, keyword_to_provider[kw], url, date_str))
                if len(found_entries) >= MAX_KEYWORDS_PER_COMPANY:
                    break

        await delay(REQUEST_DELAY)

    await page.close()

    if not found_entries:
        print(f"❌ No keywords found in {domain}")

async def main():
    await ensure_directory_exists(RESULTS_DIR)

    companies = []
    async with aiofiles.open(COMPANIES_FILE, 'r', encoding='utf-8') as f:
        async for line in f:
            line = line.strip()
            if not line:
                continue
            parts = re.split(r'[,|\t]', line)
            if len(parts) >= 2:
                url = parts[0].strip()
                country = parts[1].strip()
            elif len(parts) == 1:
                url = parts[0].strip()
                country = '-'
            else:
                print(f"⚠️ Skipping malformed line: {line!r}")
                continue
            domain = urlparse(url).netloc or url
            companies.append((domain, country))

    async with aiofiles.open(KEYWORDS_FILE, 'r') as f:
        keywords_data = json.loads(await f.read())

    all_keywords = []
    keyword_to_provider = {}
    for provider, kws in keywords_data.items():
        for kw in kws:
            clean_kw = kw.lower().strip()
            all_keywords.append(clean_kw)
            keyword_to_provider[clean_kw] = provider

    print(f" Companies to process: {len(companies)}")

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])

            for domain, country in companies:
                print(f"=== Processing: {domain} ({country}) ===")
                try:
                    await process_company(domain, country, all_keywords, keyword_to_provider, session, browser)
                except Exception as e:
                    print(f" Error on {domain}: {e}")

            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())
