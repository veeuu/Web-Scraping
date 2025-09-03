import requests
import re
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup


KEYWORD_VARIANTS = {
    "Cloud": ["Cloud"],
    "AWS": ["AWS", "Amazon Web Services"],
    "Azure": ["Azure", "Microsoft Azure"],
    "GCP": ["GCP", "Google Cloud Platform"],
    "Oracle Cloud": ["Oracle Cloud"],
    "Alibaba Cloud": ["Alibaba Cloud"],
    "Nutanix": ["Nutanix"],

    "Project Management": ["Project Management", "PMP", "PLM", "ITIL"],

    "MES": ["MES", "Manufacturing Execution System"],

    "IoT": ["IoT", "Internet of Things", "IIOT"],

    "Low-code Automation": ["Power Apps", "MS Power", "Oracle APEX", "UiPath", "Power Automate"],

    "BI Tools": ["Tableau", "Power BI", "QlikView", "Qlik Sense"],

    "CAD/CAM": [
        "Siemens NX", "SolidWorks", "AutoCAD", "CATIA", "Creo", "Autodesk", "Solid Edge",
        "SOLIDWORKS CAM", "Fusion 360", "Onshape", "SketchUp", "NX CAM",
        "CAMWorks", "SolidCAM", "Mastercam", "CAMMaster"
    ],

    "Digital Twin": ["Digital Twin"]
}

# Compile regex patterns
PATTERNS = []
for main_kw, variants in KEYWORD_VARIANTS.items():
    for v in variants:
        PATTERNS.append((main_kw, re.compile(r"\b" + re.escape(v) + r"\b", re.IGNORECASE)))


def fetch_text_with_jina(url):
    """Fetch page text via Jina AI proxy"""
    try:
        jina_url = f"https://r.jina.ai/{url}"
        resp = requests.get(jina_url, timeout=20)
        if resp.status_code == 200:
            return resp.text
    except Exception:
        return ""
    return ""


def find_keyword(text):
    """Search text for keywords"""
    for main_kw, pattern in PATTERNS:
        if pattern.search(text):
            return main_kw
    return None


def get_internal_links(base_url):
    """Get internal links from homepage"""
    links = set()
    try:
        resp = requests.get(base_url, timeout=10, headers={"User-Agent": "Mozilla/5.0"})
        if resp.status_code == 200:
            soup = BeautifulSoup(resp.text, "html.parser")
            domain = urlparse(base_url).netloc
            for a in soup.find_all("a", href=True):
                href = urljoin(base_url, a["href"])
                if urlparse(href).netloc == domain:
                    links.add(href.split("#")[0])  # remove #fragment
    except Exception:
        pass
    return links


def check_company(company_name, base_url, max_pages=10):
    result = {
        "company": company_name,
        "website": base_url,
        "usage": "no",
        "keyword": None,
        "url": None
    }

    visited = set()

    # Step 1: Check homepage
    text = fetch_text_with_jina(base_url)
    kw = find_keyword(text)
    if kw:
        result.update({"usage": "yes", "keyword": kw, "url": base_url})
        return result

    # Step 2: Crawl internal links
    internal_links = get_internal_links(base_url)
    for link in list(internal_links)[:max_pages]:  # limit for speed
        if link not in visited:
            visited.add(link)
            text = fetch_text_with_jina(link)
            kw = find_keyword(text)
            if kw:
                result.update({"usage": "yes", "keyword": kw, "url": link})
                return result

    return result


if __name__ == "__main__":
    companies = [
        ("1 Auto Cars", "http://www.1autos.com.sg/"),
        ("2DM", "http://www.2dmsolutions.com/"),
        ("338 AIRCONDITIONING SERVICES (SINGAPORE)", "http://www.338aircon.sg/"),
        ("3D Aura Pte Ltd", "http://www.3daura.com.sg/"),
        ("3D Gens Sdn Bhd", "http://www.3dgens.com/"),
        ("3P Power Pte Ltd", "http://www.3ppower.com/"),
        ("47Ronin Watch Straps", "http://www.47ronin.co/"),
        ("6K HYPERSPACE (M) SDN BHD", "http://www.hyperspace-intl.com/"),
        ("A & ONE Precision Engineering Pte Ltd", "http://www.a-oneprecision.com/"),
    ]

    for name, url in companies:
        res = check_company(name, url, max_pages=15)
        print(f"\n{res['company']} -")
        print(f" website - {res['website']}")
        print(f"Usage : {res['usage']}")
        print(f"keyword found : {res['keyword']}")
        print(f"url of found keyword : {res['url']}")
