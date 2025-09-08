import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse
import csv
import os

# =====================
# Keyword Dictionaries
# =====================
ccaas_keywords = [
    "Vonage CX", "Avaya OneCloud", "Genesys", "NICE CXone", "Five9", "Talkdesk",
    "8x8", "Amazon Connect", "Twilio Flex", "Cisco Webex Contact Center",
    "Twilio TaskRouter", "Twilio Conversations", "Twilio Autopilot", "Twilio Voice",
    "RingCentral", "Asterisk", "3CX", "Nice", "Cisco WebX", "NEC", "Telstra",
    "Alcatel Lucent", "Mitel", "NEC Cloud", "Optus", "Zendesk",
    "Microsoft Dynamics 365 Contact Center", "Zoom Contact Centre",
    "Yealink", "Yeastar", "Alcatel", "Ozonetel", "Verizon Contact Center", "Verint",
    "Tencent Cloud", "Freshdesk"
]

voice_keywords = [
    "Vonage", "Twilio", "Sinch", "Plivo", "RingCentral", "Telnyx", "Voxbone",
    "Grasshopper", "Jive Communications", "Avaya", "Nice", "Onprem", "Microsoft Teams",
    "Yealink", "Yeastar", "NEC", "Genesys", "3CX", "Mitel", "Telstra IP Telephony",
    "Vocus IP Telephony", "Cloud PBX", "Grandstream", "Dialpad", "Ring Central",
    "8x8", "Aircall", "Avaya CMS", "yellow.ai", "Verint", "Gnani.ai", "Vonage CX",
    "Oracle AI Voice", "PBX", "LG iPECS", "Polycom", "IP PBX", "Poly", "IPabx",
    "EPabx", "EPABX", "Nortel", "MS Teams", "Softphone"
]

# =====================
# URL Normalization
# =====================
def normalize_url(url):
    """Ensure the URL has a valid scheme (https:// by default)."""
    if not url:
        return None
    url = url.strip()
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url

# =====================
# Crawler Function
# =====================
def crawl_and_search(base_url, max_pages=10):
    visited = set()
    to_visit = [base_url]
    found_voice = set()
    found_ccaas = set()

    while to_visit and len(visited) < max_pages:
        url = to_visit.pop(0)
        if url in visited:
            continue
        visited.add(url)

        try:
            resp = requests.get(url, timeout=10)
            if "text/html" not in resp.headers.get("Content-Type", ""):
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            text = soup.get_text(" ", strip=True)

            # Search keywords
            for k in voice_keywords:
                if k.lower() in text.lower():
                    found_voice.add(k)

            for k in ccaas_keywords:
                if k.lower() in text.lower():
                    found_ccaas.add(k)

            # Collect internal links
            for a in soup.find_all("a", href=True):
                link = urljoin(url, a["href"])
                if urlparse(link).netloc == urlparse(base_url).netloc:
                    if link not in visited:
                        to_visit.append(link)

        except Exception as e:
            print(f"Error fetching {url}: {e}")
            continue

    return list(found_voice), list(found_ccaas)

# =====================
# Fallback search without domain
# =====================
def keyword_search_in_text(text):
    found_voice = [k for k in voice_keywords if k.lower() in text.lower()]
    found_ccaas = [k for k in ccaas_keywords if k.lower() in text.lower()]
    return found_voice, found_ccaas

# =====================
# Main Function
# =====================
def main(input_csv, output_csv):
    # If output file exists → remove (to start fresh)
    if os.path.exists(output_csv):
        os.remove(output_csv)

    with open(input_csv, newline="", encoding="utf-8") as infile:
        reader = csv.DictReader(infile)
        fieldnames = ["Company Name", "Country", "Domain",
                      "Voice Provider Keywords", "CCaaS Keywords"]

        # Create output file with header first
        with open(output_csv, "w", newline="", encoding="utf-8") as outfile:
            writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="|")
            writer.writeheader()

        # Process companies one by one
        for row in reader:
            company = row.get("Company Name", "").strip()
            country = row.get("Country", "").strip()
            website = row.get("Domain", "").strip() or row.get("Website", "").strip()

            website = normalize_url(website) if website else None

            print(f"Scanning {company} ({country}) - {website if website else 'NO DOMAIN'}")

            if website:
                voice_found, ccaas_found = crawl_and_search(website)
            else:
                search_text = f"{company} {country}"
                voice_found, ccaas_found = keyword_search_in_text(search_text)

            # Write result immediately (append mode)
            with open(output_csv, "a", newline="", encoding="utf-8") as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames, delimiter="|")
                writer.writerow({
                    "Company Name": company,
                    "Country": country,
                    "Domain": website if website else "",
                    "Voice Provider Keywords": ", ".join(sorted(set(voice_found))),
                    "CCaaS Keywords": ", ".join(sorted(set(ccaas_found)))
                })
                outfile.flush()  # ✅ flush to disk so you can see results live

# =====================
# Run Script
# =====================
if __name__ == "__main__":
    input_csv = "companies1.csv"      # <-- put your input file here
    output_csv = "output.csv"    # <-- output file
    main(input_csv, output_csv)
    print(f"\n✅ Done! Results saved in {output_csv}")
