import re
import csv
from datetime import datetime
from pathlib import Path
from io import BytesIO
import requests
from bs4 import BeautifulSoup
from PyPDF2 import PdfReader
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

INPUT_CSV = r'C:\Users\propl\OneDrive\Desktop\work\RESULTS_DIR\company_keyword_results.csv'
OUTPUT_CSV = r'C:\Users\propl\OneDrive\Desktop\work\company_keyword_results_with_dates.csv'

NOW = datetime.now()
CURRENT_YEAR, CURRENT_MONTH = NOW.year, NOW.month

MONTH_MAP = {
    'january': 1, 'february': 2, 'march': 3, 'april': 4,
    'may': 5, 'june': 6, 'july': 7, 'august': 8,
    'september': 9, 'october': 10, 'november': 11, 'december': 12
}
MONTH_REGEX = '|'.join(MONTH_MAP)

DATE_PATTERNS = [
    re.compile(rf'({MONTH_REGEX})\s+(\d{{1,2}})(?:st|nd|rd|th)?,?\s*(20\d{{2}})', re.I),  # September 3rd, 2024
    re.compile(rf'({MONTH_REGEX})\s*(20\d{{2}})', re.I),                                # September 2024
    re.compile(r'(\d{1,2})[-/](\d{1,2})[-/](20\d{2})'),                                 # 01-02-2024
    re.compile(r'(20\d{2})[./-](\d{1,2})[./-](\d{1,2})'),                               # 2024-01-01
    re.compile(r'\b(19\d{2}|20\d{2})\b')                                                # just year
]

def parse_date(text):
    text = text.lower()
    for pattern in DATE_PATTERNS:
        match = pattern.search(text)
        if match:
            g = match.groups()
            try:
                if len(g) == 3:
                    if g[0] in MONTH_MAP:  # September 3 2024
                        m = MONTH_MAP[g[0]]
                        return f"{m:02d} {g[2]}"
                    else:  # 01-02-2024
                        return f"{int(g[1]):02d} {g[2]}"
                elif len(g) == 2 and g[0] in MONTH_MAP:  # September 2024
                    m = MONTH_MAP[g[0]]
                    return f"{m:02d} {g[1]}"
                elif len(g) == 1:  # just year
                    y = int(g[0])
                    if 1900 <= y <= CURRENT_YEAR:
                        return f"{CURRENT_MONTH:02d} {y}"
            except Exception:
                continue
    return None

def fetch_page_content(url):
    try:
        resp = requests.get(url, timeout=60, headers={"User-Agent": "Mozilla/5.0"}, verify=False)
        if resp.status_code != 200:
            return None, "file_not_found"
        ct = resp.headers.get("content-type", "").lower()
        if "pdf" in ct or url.lower().endswith(".pdf"):
            return resp.content, "pdf"
        return resp.text, "html"
    except Exception as e:
        print(f"❌ Failed to fetch {url}: {e}")
        return None, "load_failed"

def extract_date_from_pdf(content):
    try:
        pdf = PdfReader(BytesIO(content))
        info = pdf.metadata
        for attr in ['/ModDate', '/CreationDate']:
            if attr in info and info[attr]:
                if (d := parse_date(info[attr])):
                    return d, "pdf_metadata"
        # fallback: text
        for page in pdf.pages:
            txt = page.extract_text() or ''
            if (d := parse_date(txt)):
                return d, "pdf_text"
    except Exception as e:
        print(f"❌ PDF error: {e}")
    return None, "pdf_no_date"

def extract_date_from_html(content):
    soup = BeautifulSoup(content, 'html.parser')

    # priority selectors
    for selector in [
        '.local-date', '.pr-date', 'time', 'meta[name="pubdate"]',
        'meta[property="article:published_time"]'
    ]:
        el = soup.select_one(selector)
        if el:
            date_text = el.get_text() if el.name != 'meta' else el.get('content', '')
            if (d := parse_date(date_text)):
                return d, selector

    # look in title & headings
    for tag in ['title', 'h1', 'h2', 'h3']:
        for el in soup.find_all(tag):
            if (d := parse_date(el.get_text())):
                return d, tag

    # look in copyright
    copyright_patterns = [
        re.compile(r'©\s*(19\d{2}|20\d{2})'),
        re.compile(r'copyright\s+©?\s*(19\d{2}|20\d{2})', re.I)
    ]
    text = soup.get_text(" ", strip=True)
    for pattern in copyright_patterns:
        match = pattern.search(text)
        if match:
            y = match.group(1)
            return f"{CURRENT_MONTH:02d} {y}", "copyright"

    # fallback to body text
    if (d := parse_date(text)):
        return d, "body_text"

    return None, "not_found"

def get_date(url):
    content, ctype = fetch_page_content(url)

    if ctype in ("load_failed", "file_not_found"):
        return None, ctype

    if ctype == "pdf":
        return extract_date_from_pdf(content)

    if ctype == "html":
        return extract_date_from_html(content)

    return None, "unknown"

def main():
    input_path = Path(INPUT_CSV)
    output_path = Path(OUTPUT_CSV)

    with open(input_path, newline='', encoding='utf-8') as fin, \
         open(output_path, 'w', newline='', encoding='utf-8') as fout:

        reader = csv.DictReader(fin)
        writer = csv.DictWriter(fout, fieldnames=reader.fieldnames + ['date', 'date_source'])
        writer.writeheader()

        for row in reader:
            url = row['URL']
            print(f"📄 Processing: {url}")
            date, src = get_date(url)
            row['date'] = date or ''
            row['date_source'] = src
            writer.writerow(row)

    print(f"Dates written to {output_path}")

if __name__ == "__main__":
    main()