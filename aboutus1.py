import requests
from bs4 import BeautifulSoup
import re
import spacy

nlp = spacy.load("en_core_web_sm")

DESIGNATION_KEYWORDS = [
    "Chief", "CEO", "CFO", "COO", "CIO", "CTO",
    "President", "Vice President", "VP", "CRO",
    "Managing Director", "Founder", "Co-Founder",
    "Partner", "Board Member", "Head", "Chairman",
    "Director", "Officer"
]

BLACKLIST = {"Support", "Sitemap", "Partners", "Resources", "Company", "Home", "Headquarters"}


def fetch_html(url):
    try:
        r = requests.get(url, timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"Direct fetch failed: {e}")
    try:
        r = requests.get(f"https://r.jina.ai/{url}", timeout=10)
        if r.status_code == 200:
            return r.text
    except Exception as e:
        print(f"Jina fetch failed: {e}")
    return ""


def is_valid_name(name: str) -> bool:
    """Ensure name looks like a proper person (not title or noise)."""
    parts = name.strip().split()
    if len(parts) < 2 or len(parts) > 3:
        return False
    if any(p in BLACKLIST for p in parts):
        return False
    if any(p in DESIGNATION_KEYWORDS for p in parts):  # filter “VP Engineering”, “Chief Tech”
        return False
    return all(p[0].isupper() and p.isalpha() for p in parts if p)


def clean_designation(text: str) -> str:
    """Return a clean short job title (no biography)."""
    for keyword in DESIGNATION_KEYWORDS:
    
        match = re.search(rf"\b{keyword}(\s+[A-Z][a-zA-Z]+){{0,3}}", text, re.IGNORECASE)
        if match:
            return match.group().strip()
    return ""


def extract_executives(html):
    soup = BeautifulSoup(html, "html.parser")
    text_blocks = soup.find_all(["h1", "h2", "h3", "h4", "p", "div", "span", "li"])

    exec_map = {}  # name -> set(designations)

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


    executives = [{"name": name, "designations": sorted(list(desigs))}
                  for name, desigs in exec_map.items()]

    return executives


if __name__ == "__main__":
    urls = [
        "http://www.215marketing.com",
        "https://3analytics.com/aboutus/",
        "https://1datapipe.com/executive-team/"
    ]

    for url in urls:
        print(f"\n---- {url} ----")
        html = fetch_html(url)
        executives = extract_executives(html)
        for ex in executives:
            print(ex)
