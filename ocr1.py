import csv
import re
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
from PIL import Image
import pytesseract
from io import BytesIO
import pandas as pd
import json
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed


INPUT_FILE = "OS_Test.csv"
OUTPUT_FILE = "ocr_results.csv"
CHECKPOINT_DIR = "checkpoints"
KEYWORDS_FILE = "os_keywords.json"
BATCH_SIZE = 300
MAX_WORKERS = 50

with open(KEYWORDS_FILE, "r", encoding="utf-8") as f:
    data = json.load(f)
if isinstance(data, dict):
    OS_KEYWORDS = [kw for v in data.values() for kw in (v if isinstance(v, list) else [v])]
else:
    OS_KEYWORDS = data
print(f"Loaded {len(OS_KEYWORDS)} keywords from {KEYWORDS_FILE}")


ALLOWED_EXTENSIONS = (".jpg", ".jpeg", ".png", ".gif", ".bmp", ".tiff", ".tif", ".webp", ".svg")


os.makedirs(CHECKPOINT_DIR, exist_ok=True)

session = requests.Session()
session.headers.update({"User-Agent": "Mozilla/5.0 (OCRBot)"})

def extract_text_from_images(website, keywords, company, domain, country):
    results = []
    try:
        response = session.get(website, timeout=15)
        response.raise_for_status()
        soup = BeautifulSoup(response.text, 'html.parser')

        for img_tag in soup.find_all("img"):
            img_src = img_tag.get("src")
            if not img_src:
                continue
            if not img_src.lower().endswith(ALLOWED_EXTENSIONS):
                continue

            img_url = urljoin(website, img_src)

            try:
                if img_url.lower().endswith(".svg"):
                    svg_text = session.get(img_url, timeout=10).text
                    cleaned_text = re.sub(r'[^A-Za-z0-9\s]', ' ', svg_text)
                    cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()
                    found = [kw for kw in keywords if kw.lower() in cleaned_text.lower()]
                    if found:
                        results.append({
                            "Company": company,
                            "Domain": domain,
                            "Country": country,
                            "Website": website,
                            "Image_URL": img_url,
                            "Extracted_Text": "(SVG Text Found)",
                            "Found_Keywords": ", ".join(found)
                        })
                    continue

                # Raster OCR
                img_resp = session.get(img_url, timeout=10)
                img_resp.raise_for_status()
                img = Image.open(BytesIO(img_resp.content))

                if img.width < 50 or img.height < 50:
                    continue

                text = pytesseract.image_to_string(img).strip()
                cleaned_text = re.sub(r'[^A-Za-z0-9\s]', ' ', text)
                cleaned_text = re.sub(r'\s+', ' ', cleaned_text).strip()

                found = [kw for kw in keywords if kw.lower() in cleaned_text.lower()]
                if found:
                    results.append({
                        "Company": company,
                        "Domain": domain,
                        "Country": country,
                        "Website": website,
                        "Image_URL": img_url,
                        "Extracted_Text": cleaned_text,
                        "Found_Keywords": ", ".join(found)
                    })
            except Exception:
                continue
    except Exception:
        pass
    return results


def process_row(row, processed_domains):
    company = row.get("Company Name", "").strip()
    domain = row.get("Domain", "").strip()
    country = row.get("Country", "").strip()

    if not domain or domain in processed_domains:
        return []

    website = f"https://{domain}"
    print(f"🔍 Processing: {company} ({domain}, {country})")
    return extract_text_from_images(website, OS_KEYWORDS, company, domain, country)


def main():
    results = []
    processed_domains = set()

    with open(INPUT_FILE, "r", encoding="utf-8") as f:
        reader = list(csv.DictReader(f))
    
    total = len(reader)
    print(f"📌 Total domains to process: {total}")

    batch_count = 0
    start_time = time.time()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(process_row, row, processed_domains): row for row in reader}
        for i, future in enumerate(as_completed(futures), start=1):
            try:
                site_results = future.result()
                results.extend(site_results)

                domain = futures[future].get("Domain", "").strip()
                if domain:
                    processed_domains.add(domain)
            except Exception:
                continue


            if i % BATCH_SIZE == 0:
                checkpoint_file = os.path.join(CHECKPOINT_DIR, f"ocr_checkpoint_{i}.csv")
                pd.DataFrame(results).to_csv(checkpoint_file, index=False, encoding="utf-8")
                print(f"💾 Checkpoint saved: {checkpoint_file} ({i}/{total})")


    if results:
        pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False, encoding="utf-8")
        print(f" Final results saved in {OUTPUT_FILE}")
    else:
        print(" No keywords found in images.")

    print(f"Total time: {round(time.time() - start_time, 2)} seconds")

if __name__ == "__main__":
    main()
