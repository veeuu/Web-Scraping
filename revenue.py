import re
import requests
from bs4 import BeautifulSoup
from googlesearch import search
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

def extract_revenue_from_text(text):
    """
    Look for proper revenue mentions only.
    Must have currency + scale (million, billion, crore, etc.)
    """
    revenue_patterns = [
        r"\$[0-9][\d,\. ]+ ?(million|billion|trillion|bn|m|t)?",   # $12 million
        r"USD ?[0-9][\d,\. ]+ ?(million|billion|trillion|bn|m|t)?",
        r"€[0-9][\d,\. ]+ ?(million|billion|trillion|bn|m|t)?",
        r"£[0-9][\d,\. ]+ ?(million|billion|trillion|bn|m|t)?",
        r"₹ ?[0-9][\d,\. ]+ ?(crore|lakh|cr|million|billion)?",
        r"[0-9][\d,\. ]+ ?(million|billion|trillion|crore|lakh|bn|m|t) ?(usd|dollars|rs|inr|rupees)?"
    ]
    for pattern in revenue_patterns:
        match = re.search(pattern, text, re.IGNORECASE)
        if match:
            return match.group(0)
    return None


def get_company_revenue(company_name, industry, website):
    """
    Search Google for company revenue and scrape pages.
    """
    query = f"{company_name} {industry} {website} revenue annual sales turnover"
    print(f"\n🔍 Searching Google for: {query}\n")

    try:
        results = search(query, num=6, stop=6, pause=2)
    except Exception as e:
        return "Search failed"

    for url in results:
        try:
            print(f"Checking: {url}")
            headers = {"User-Agent": "Mozilla/5.0"}
            page = requests.get(url, headers=headers, timeout=10)
            soup = BeautifulSoup(page.text, "html.parser")

            # Extract visible text
            paragraphs = soup.find_all(["p", "span", "li", "div"])
            for para in paragraphs:
                text = para.get_text(" ", strip=True)
                if any(word in text.lower() for word in ["revenue", "sales", "turnover"]):
                    revenue = extract_revenue_from_text(text)
                    if revenue:
                        return f"{revenue} (from {url})"
        except Exception:
            continue

    return "Revenue not found"


def process_company(row):
    """Process one company row from CSV."""
    company_name, industry, website = row["Company"], row["Industry"], row["Website"]
    revenue = get_company_revenue(company_name, industry, website)
    return {
        "Company": company_name,
        "Industry": industry,
        "Website": website,
        "Revenue": revenue
    }


if __name__ == "__main__":
    # Read input CSV
    input_file = "revenue_sheet.csv"
    output_file = "revenue_output.csv"

    df = pd.read_csv(input_file)

    results = []
    with ThreadPoolExecutor(max_workers=5) as executor:
        futures = [executor.submit(process_company, row) for _, row in df.iterrows()]
        for future in as_completed(futures):
            results.append(future.result())

    # Save results to CSV
    output_df = pd.DataFrame(results)
    output_df.to_csv(output_file, index=False)

    print(f"\n✅ Results saved to {output_file}")
