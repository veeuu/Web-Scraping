import asyncio
import json
from io import BytesIO
from pathlib import Path
from urllib.parse import quote, urlparse
import aiofiles
import aiohttp
from bs4 import BeautifulSoup
from docx import Document
import re
from datetime import datetime
from playwright.async_api import async_playwright
import ssl
from tempfile import NamedTemporaryFile
import pikepdf
from pdfminer.high_level import extract_text as pdfminer_extract_text
import openpyxl
import os
from dotenv import load_dotenv
load_dotenv()

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


SCRAPINGDOG_API_KEY = os.getenv('SCRAPINGDOG_API_KEY')
SEARCH_QUERY_TEMPLATE = (
    '"{company}" "{country}" AND ({keyword}) AND (partnership OR collaboration OR alliance OR agreement OR deal OR relationship)'
)

ALLOWED_THIRD_PARTY_PATTERNS = [
    r'aws\.amazon\.com/.*/case-studies/',
    r'aws\.amazon\.com/blogs/',
    r'cloud\.google\.com/customers/',
    r'cloudblogs\.microsoft\.com/',
    r'customers\.microsoft\.com/',
    r'blogs\.oracle\.com/',
    r'blog\.[\w\-]+\.com/',
    r'cloud.*\.com/',
]

COMPANIES_FILE = r'input.csv'
KEYWORDS_FILE = r'aws_keywords.json'
RESULTS_DIR = Path(r'RESULTS_DIR')
RESULTS_CSV = r'results0.csv'

REQUEST_DELAY = 1.5
MAX_KEYWORDS_PER_COMPANY = 3

ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE


def sanitize_filename(name: str) -> str:
    return re.sub(r'[\\/:*?"<>|]', '_', name)

def get_canonical_domain(url_or_domain: str) -> str:
    parsed = urlparse(url_or_domain)
    domain = parsed.netloc or parsed.path
    return domain.lower().replace("www.", "")

def is_allowed_third_party(url):
    return any(re.search(pat, url) for pat in ALLOWED_THIRD_PARTY_PATTERNS)

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
                    return f"12 {y}"
    return None

async def ensure_directory_exists(path: Path):
    path.mkdir(parents=True, exist_ok=True)

async def delay(seconds):
    await asyncio.sleep(seconds)


def extract_text_from_xlsx(data):
    text_parts = []
    with NamedTemporaryFile(delete=True, suffix=".xlsx") as tmp:
        tmp.write(data)
        tmp.flush()
        wb = openpyxl.load_workbook(tmp.name, read_only=True)
        for sheet in wb.worksheets:
            for row in sheet.iter_rows(values_only=True):
                text_parts.append(' '.join(str(cell) for cell in row if cell))
    return '\n'.join(text_parts)

async def fetch_text(session, url):
    try:
        async with session.get(url, timeout=20, ssl=ssl_context) as resp:
            content_type = resp.headers.get('content-type', '')
            data = await resp.read()

            if 'pdf' in content_type or url.endswith(".pdf"):
                with NamedTemporaryFile(delete=True, suffix=".pdf") as tmp:
                    tmp.write(data)
                    tmp.flush()
                    with pikepdf.open(tmp.name) as pdf:
                        pdf.save(tmp.name)
                    text = pdfminer_extract_text(tmp.name)
                    return text

            if 'word' in content_type or url.endswith(".docx"):
                doc = Document(BytesIO(data))
                return '\n'.join(p.text for p in doc.paragraphs)

            if 'excel' in content_type or url.endswith(".xlsx"):
                return extract_text_from_xlsx(data)

            soup = BeautifulSoup(data, 'html.parser')
            return soup.get_text()
    except Exception as e:
        print(f"Failed to fetch {url}: {e}")
        return None

async def fetch_with_playwright(page, url):
    try:
        if url.endswith(".pdf") or url.endswith(".xlsx"):
            return None
        await page.goto(url, wait_until='networkidle', timeout=45000)
        text = await page.evaluate("document.body.innerText")
        return text
    except Exception as e:
        print(f"Playwright failed on {url}: {e}")
        return None

async def crawl_urls(session, browser, start_urls, official_domain=None, max_depth=2):
    visited = set()
    queue = [(url, 0) for url in start_urls]
    all_texts = {}  # url → text

    while queue:
        url, depth = queue.pop(0)
        if url in visited or depth > max_depth:
            continue
        visited.add(url)

        page_text = await fetch_text(session, url)
        if not page_text:
            page = await browser.new_page()
            page_text = await fetch_with_playwright(page, url)
            await page.close()

        if page_text:
            all_texts[url] = page_text

            if depth < max_depth:
                soup = BeautifulSoup(page_text, 'html.parser')
                for a in soup.find_all('a', href=True):
                    next_url = a['href']
                    if next_url.startswith('/'):
                        parsed = urlparse(url)
                        next_url = f"{parsed.scheme}://{parsed.netloc}{next_url}"
                    if next_url.startswith('http') and next_url not in visited:
                        # prioritize official domain
                        if official_domain and get_canonical_domain(next_url) != official_domain:
                            continue
                        queue.append((next_url, depth+1))

        await delay(REQUEST_DELAY)

    return all_texts


async def perform_google_search(session, company, country, search_query):
    url = f"https://api.scrapingdog.com/google?api_key={SCRAPINGDOG_API_KEY}&query={quote(search_query)}"
    print(f"🔍 Searching: '{search_query}'")
    try:
        async with session.get(url, timeout=30, ssl=ssl_context) as resp:
            data = await resp.json()
            file_path = RESULTS_DIR / f"{sanitize_filename(company)}.json"
            async with aiofiles.open(file_path, 'w') as f:
                await f.write(json.dumps(data, indent=2))
            return data
    except Exception as e:
        print(f"Error searching {company}: {e}")
        return None

def extract_urls(search_data):
    results = search_data.get('organic_results') or search_data.get('organic_data')
    if not results:
        return []
    return [r.get('link') for r in results if r.get('link')]

async def append_final_csv_row(company, prev_kw, prev_url, prev_date, latest_kw, latest_url, latest_date):
    async with aiofiles.open(RESULTS_CSV, 'a') as f:
        await f.write(f"{company},{prev_kw or '-'}," +
                      f"{prev_url or '-'}," +
                      f"{prev_date or '-'}," +
                      f"{latest_kw or '-'}," +
                      f"{latest_url or '-'}," +
                      f"{latest_date or '-'}\n")

async def process_company(company, country, all_keywords, keyword_to_provider, session, browser):
    found_internal = []
    official_domain = None
    page = await browser.new_page()

    all_kw_query = " OR ".join([f'"{kw}"' for kw in all_keywords])
    search_query = SEARCH_QUERY_TEMPLATE.format(company=company, country=country, keyword=all_kw_query)

    search_data = await perform_google_search(session, company, country, search_query)
    if not search_data:
        await append_final_csv_row(company, '', '', '', '', '', '')
        await page.close()
        return

    urls = extract_urls(search_data)
    if not urls:
        await append_final_csv_row(company, '', '', '', '', '', '')
        await page.close()
        return

    # prioritize company official site
    for u in urls:
        if company.replace(" ", "").lower() in get_canonical_domain(u):
            official_domain = get_canonical_domain(u)
            break

    crawled_texts = await crawl_urls(session, browser, urls, official_domain, max_depth=2)

    for url, text in crawled_texts.items():
        if len(found_internal) >= MAX_KEYWORDS_PER_COMPANY:
            break
        lower_text = text.lower()
        for kw in all_keywords:
            if re.search(rf"\b{re.escape(kw)}\b", lower_text):
                found_internal.append((kw, keyword_to_provider[kw], url))
                break

    await page.close()

    if not found_internal:
        await append_final_csv_row(company, '', '', '', '', '', '')
        return

    dated_links = []
    for kw, provider, url in found_internal:
        date_str = parse_date(crawled_texts[url])
        year = int(date_str.split()[1]) if date_str else 0
        dated_links.append((kw, provider, url, date_str, year))

    prev, latest = None, None
    for entry in dated_links:
        if entry[4] == CURRENT_YEAR:
            if latest is None or (entry[3] and entry[3] > latest[3]):
                latest = entry
        elif entry[4] < CURRENT_YEAR:
            if prev is None or (entry[3] and entry[3] > prev[3]):
                prev = entry

    if prev is None:
        prev = ('-', '-', '-', '-', '-')
    if latest is None:
        latest = ('-', '-', '-', '-', '-')

    await append_final_csv_row(
        company,
        f"{prev[1]}: {prev[0]}", prev[2], prev[3],
        f"{latest[1]}: {latest[0]}", latest[2], latest[3]
    )


async def main():
    await ensure_directory_exists(RESULTS_DIR)

    if not Path(RESULTS_CSV).exists():
        async with aiofiles.open(RESULTS_CSV, 'w') as f:
            await f.write('company,keyword of previous,previous detected link,previous detected date,keyword of latest,latest detected link,latest detected date\n')

    processed = set()
    async with aiofiles.open(RESULTS_CSV, 'r') as f:
        async for line in f:
            processed.add(line.strip().split(',')[0])

    companies = []
    async with aiofiles.open(COMPANIES_FILE, 'r') as f:
        async for line in f:
            parts = line.strip().split(',')
            if len(parts) >= 2:
                companies.append((parts[0].strip(), parts[1].strip()))

    async with aiofiles.open(KEYWORDS_FILE, 'r') as f:
        keywords_data = json.loads(await f.read())

    all_keywords = []
    keyword_to_provider = {}
    for provider, kws in keywords_data.items():
        for kw in kws:
            clean_kw = kw.lower().strip()
            all_keywords.append(clean_kw)
            keyword_to_provider[clean_kw] = provider

    async with aiohttp.ClientSession() as session:
        async with async_playwright() as pw:
            browser = await pw.chromium.launch(headless=True, args=['--no-sandbox', '--ignore-certificate-errors'])

            for company, country in companies:
                if company in processed:
                    print(f"Skipping {company}")
                    continue
                try:
                    await process_company(company, country, all_keywords, keyword_to_provider, session, browser)
                except Exception as e:
                    print(f"Error on {company}: {e}")

            await browser.close()

if __name__ == "__main__":
    asyncio.run(main())