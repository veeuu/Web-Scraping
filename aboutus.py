import requests
from bs4 import BeautifulSoup
import re
import spacy
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
import os
import traceback

# Load spaCy model
nlp = spacy.load("en_core_web_sm")

DESIGNATION_KEYWORDS = [
    "Chief", "CEO", "CFO", "COO", "CIO", "CTO",
    "President", "Vice President", "VP", "CRO",
    "Managing Director", "Founder", "Co-Founder",
    "Partner", "Board Member", "Head", "Chairman",
    "Director", "Officer"
]

BLACKLIST = {"Support", "Sitemap", "Partners", "Resources", "Company", "Home", "Headquarters"}

OUTPUT_FILE = "executives_results.csv"
CHECKPOINT_DIR = "checkpoints(pycharm)"
BATCH_SIZE = 200
THREADS = 5

os.makedirs(CHECKPOINT_DIR, exist_ok=True)


def fetch_html(url):
    try:
        r = requests.get(url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if r.status_code == 200:
            return r.text
    except Exception:
        pass

    try:
        r = requests.get(f"https://r.jina.ai/{url}", timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception:
        pass
    return ""


def is_valid_name(name: str) -> bool:
    parts = name.strip().split()
    if len(parts) < 2 or len(parts) > 3:
        return False
    if any(p in BLACKLIST for p in parts):
        return False
    if any(p in DESIGNATION_KEYWORDS for p in parts):
        return False
    return all(p[0].isupper() and p.isalpha() for p in parts if p)


def clean_designation(text: str) -> str:
    for keyword in DESIGNATION_KEYWORDS:
        match = re.search(rf"\b{keyword}(\s+[A-Z][a-zA-Z]+){{0,3}}", text, re.IGNORECASE)
        if match:
            return match.group().strip()
    return ""


def extract_executives(html):
    soup = BeautifulSoup(html, "html.parser")
    text_blocks = soup.find_all(["h1", "h2", "h3", "h4", "p", "div", "span", "li"])

    exec_map = {}

    for block in text_blocks:
        text = block.get_text(" ", strip=True)
        if not text or len(text.split()) < 2:
            continue

        doc = nlp(text)
        persons = [ent.text.strip() for ent in doc.ents if ent.label_ == "PERSON"]

        for person in persons:
            if not is_valid_name(person):
                continue
            designation = clean_designation(text)
            if not designation:
                continue

            if person not in exec_map:
                exec_map[person] = set()
            exec_map[person].add(designation)

    return [{"name": n, "designations": ", ".join(sorted(list(d)))} for n, d in exec_map.items()]


def find_about_pages(base_url):
    html = fetch_html(base_url)
    if not html:
        return []

    soup = BeautifulSoup(html, "html.parser")
    candidate_links = []
    keywords_priority = [
        ("leadership", 5), ("executive", 5), ("management", 5),
        ("team", 4), ("board", 4), ("about", 3),
        ("company", 2), ("who-we-are", 2),
    ]

    for a in soup.find_all("a", href=True):
        href = a["href"].lower()
        text = a.get_text(" ", strip=True).lower()

        if href.startswith("http") and not href.startswith(base_url):
            continue

        for kw, score in keywords_priority:
            if kw in href or kw in text:
                full_url = requests.compat.urljoin(base_url, a["href"])
                candidate_links.append((score, full_url))
                break

    candidate_links.sort(reverse=True)
    return [link for _, link in candidate_links]


def normalize_url(raw_url: str):
    """Ensure https/http prefix, return both www and non-www versions"""
    url = raw_url.strip()

    if not url.startswith("http"):
        url = "https://" + url
    url = re.sub(r"^https?://www\.", "https://", url)

    # make both variants
    no_www = url
    with_www = re.sub(r"^https://", "https://www.", no_www)

    return [no_www, with_www]


def process_company(base, idx):
    best_execs = []
    best_about = ""
    best_count = 0
    chosen_url = ""

    # try both www and non-www
    for url in base:
        try:
            about_pages = find_about_pages(url)
            if not about_pages:
                continue

            for about_url in about_pages[:15]:
                html = fetch_html(about_url)
                executives = extract_executives(html)
                if len(executives) > best_count:
                    best_execs = executives
                    best_about = about_url
                    best_count = len(executives)
                    chosen_url = url
        except Exception as e:
            traceback.print_exc()
            continue

    if not best_execs:
        return [(idx, base[0], "", "", "No executives detected")]

    rows = []
    for ex in best_execs:
        rows.append((idx, chosen_url, best_about, ex["name"], ex["designations"]))
    return rows


def save_checkpoint(batch_idx, results):
    df = pd.DataFrame(results, columns=["Index", "BaseURL", "AboutPage", "Name", "Designation"])
    batch_file = os.path.join(CHECKPOINT_DIR, f"checkpoint_batch_{batch_idx}.csv")
    df.to_csv(batch_file, index=False)
    mode = "a" if os.path.exists(OUTPUT_FILE) else "w"
    header = not os.path.exists(OUTPUT_FILE)
    df.to_csv(OUTPUT_FILE, mode=mode, header=header, index=False)


if __name__ == "__main__":
    df = pd.read_csv("Pathos Communication Account List.csv")

    # Skip first 600 rows
    df = df.iloc[1400:].reset_index(drop=True)

    base_urls = [normalize_url(u) for u in df["Website"].dropna().unique()]

   
    batch_idx = 1400 // BATCH_SIZE

    for start in range(0, len(base_urls), BATCH_SIZE):
        batch_urls = base_urls[start:start + BATCH_SIZE]
        print(f"\n=== Processing batch {batch_idx} ({len(batch_urls)} companies) ===")

        results = []
        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            futures = {executor.submit(process_company, base, 1400 + start + i): (1400 + start + i, base)
                       for i, base in enumerate(batch_urls)}
            for future in as_completed(futures):
                rows = future.result()
                results.extend(rows)

                
                save_checkpoint(batch_idx, results)
                results = []

        batch_idx += 1
