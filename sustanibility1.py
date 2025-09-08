import asyncio
import csv
from playwright.async_api import async_playwright
from urllib.parse import urlparse
import re

# ✅ Only R&D related keywords
KEYWORD_VARIANTS = {
    "R&D": [
        "R&D",
        "Research and Development",
        "Research & Development",
        "Innovation Center",
        "Innovation Lab",
        "Product Development",
        "Technology Development",
        "R and D",
        "Research Center",
        "Development Division"
    ]
}


# Compile regex patterns
PATTERNS = []
for main_kw, variants in KEYWORD_VARIANTS.items():
    for v in variants:
        PATTERNS.append((main_kw, re.compile(r"\b" + re.escape(v) + r"\b", re.IGNORECASE)))


async def crawl_company(company_name, base_url, semaphore, max_pages=10):
    async with semaphore:  # limit concurrency to 4
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

                # Search for keyword
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

            # ✅ If website not found, search by company name
            if not base_url or base_url.lower() == "not found":
                search_url = f"https://www.google.com/search?q={company_name}"
                await page.goto(search_url)
                try:
                    first_link = await page.eval_on_selector(
                        "a",
                        "els => els.find(e => e.href && e.href.startswith('http'))?.href"
                    )
                    if first_link:
                        base_url = first_link
                        result["website"] = base_url
                except Exception:
                    pass

            if base_url and base_url != "not found":
                await crawl_page(page, base_url)

            await browser.close()

        print(f"\n{result['company']} -")
        print(f"--> website - {result['website']}")
        print(f"Usage : {result['usage']}")
        print(f"keyword found : {result['keyword']}")
        print(f"url of found keyword : {result['url']}")

        return result


async def main():
    # ✅ Read companies.csv
    companies = []
    with open("companies.csv", newline="", encoding="latin-1") as f:
        reader = csv.DictReader(f)
        for row in reader:
            companies.append((row["Company Name"], row["Website"]))

   
    semaphore = asyncio.Semaphore(4)

    # Crawl with concurrency
    tasks = [crawl_company(name, url, semaphore) for name, url in companies]
    results = await asyncio.gather(*tasks)

    # ✅ Save to output.csv
    with open("output_siemens.csv", "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["Company Name", "Website", "Usage", "Keyword", "URL"])
        writer.writeheader()
        for r in results:
            writer.writerow({
                "Company Name": r["company"],
                "Website": r["website"],
                "Usage": r["usage"],
                "Keyword": r["keyword"],
                "URL": r["url"]
            })


if __name__ == "__main__":
    asyncio.run(main())
