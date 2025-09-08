import asyncio
from playwright.async_api import async_playwright
from urllib.parse import urlparse
import re

# Define keywords with abbreviations + full forms
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

    # ✅ Updated CAD/CAM keywords
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


async def crawl_company(company_name, base_url, max_pages=10):
    visited = set()
    result = {
        "company": company_name,
        "website": base_url,
        "usage": "no",
        "keyword": None,
        "url": None
    }

    async def crawl_page(page, url):
        if len(visited) >= max_pages or result["usage"] == "yes":
            return
        if url in visited:
            return

        visited.add(url)
        try:
            await page.goto(url, timeout=15000)
            text = await page.inner_text("body")

            # Search for any keyword variant
            for main_kw, pattern in PATTERNS:
                if pattern.search(text):
                    result["usage"] = "yes"
                    result["keyword"] = main_kw
                    result["url"] = url
                    return

            # Collect internal links
            domain = urlparse(base_url).netloc
            links = await page.eval_on_selector_all("a[href]", "els => els.map(e => e.href)")
            for link in links:
                if urlparse(link).netloc == domain and link not in visited:
                    await crawl_page(page, link)

        except Exception:
            pass

    async with async_playwright() as p:
        browser = await p.chromium.launch(headless=True)
        page = await browser.new_page()
        await crawl_page(page, base_url)
        await browser.close()

    # Print results
    print(f"\n{result['company']} -")
    print(f"--> website - {result['website']}")
    print(f"Usage : {result['usage']}")
    print(f"keyword found : {result['keyword']}")
    print(f"url of found keyword : {result['url']}")

    return result


async def main():
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

    tasks = [crawl_company(name, url) for name, url in companies]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    asyncio.run(main())
