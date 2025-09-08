import os
import asyncio
import logging
import pandas as pd
import httpx
from bs4 import BeautifulSoup
import trafilatura
import spacy
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer
import subprocess
import json

# ---------- Setup ----------
logging.basicConfig(level=logging.INFO, format="%(asctime)s — %(levelname)s — %(message)s")

nlp = spacy.load("en_core_web_sm")
nltk.download("vader_lexicon", quiet=True)
sia = SentimentIntensityAnalyzer()

# ---------- Call mistral ----------
def ask_mistral(prompt: str) -> dict:
    """Send structured prompt to local mistral via Ollama and parse JSON response."""
    try:
        result = subprocess.run(
            ["ollama", "run", "mistral"],
            input=prompt.encode("utf-8"),
            capture_output=True,
            check=True
        )
        raw = result.stdout.decode("utf-8").strip()
        # try to extract JSON
        try:
            parsed = json.loads(raw)
            return parsed
        except json.JSONDecodeError:
            logging.warning("Mistral did not return valid JSON, fallback to raw text")
            return {"uses_tech": False, "explanation": raw, "confidence": "low"}
    except Exception as e:
        logging.error(f"Ollama mistral error: {e}")
        return {"uses_tech": False, "explanation": "LLM call failed", "confidence": "low"}

# ---------- Scraping ----------
async def fetch_url(url: str) -> str:
    try:
        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(url)
            resp.raise_for_status()
            return resp.text
    except Exception as e:
        logging.error(f"Fetch failed for {url}: {e}")
        return ""

def extract_clean_text(html: str) -> str:
    if not html:
        return ""
    downloaded = trafilatura.extract(html, include_comments=False, include_tables=False)
    if downloaded:
        return downloaded
    soup = BeautifulSoup(html, "html.parser")
    return soup.get_text(separator=" ", strip=True)

# ---------- QC Prompt ----------
def build_prompt(company_name: str, keyword_tech: str, text_chunk: str) -> str:
    return f"""
You are a highly skeptical web scraper and technology analyst. 
Your goal is to decide if **{company_name}** is **currently and actively** using, deploying, supporting, or integrating the technology **'{keyword_tech}'**, based only on the provided text snippet.

---
### Step 1: Context Validation
- **Company Attribution:** Verify the text is about {company_name}. If the company is not the subject, immediately return false.
- **Keyword Context:** If '{keyword_tech}' is a common word (e.g., Glue, Shield) or acronym (e.g., S3, IAM), confirm the context is about technology infrastructure. If unclear, default to false.

### Step 2: Positive Evidence (Answer true only if these appear clearly)
- Direct statements: "We use...", "Our platform is built on...", "We deployed..."
- Job postings from {company_name} requiring '{keyword_tech}' skills for internal projects
- Partnership/case studies where {company_name} is adopting '{keyword_tech}'
- Company-authored technical blogs or docs describing use of '{keyword_tech}'

### Step 3: Exclusion Rules (Default false if any apply)
- **Speculation:** "Might use", "plans to adopt", "exploring" → false
- **Employee Skills:** Individual skills/certifications not tied to company projects → false
- **Third-party Actions:** Text describing a customer/partner’s use, not {company_name} → false
- **Training-only:** Purely offering training/certification on '{keyword_tech}' without operational use → false
- **Generic Mentions:** "Technologies like {keyword_tech}" without company attribution → false
- **Wrong Context:** If '{keyword_tech}' refers to a non-technical meaning (e.g., glue = adhesive) → false

---
### Text to Analyze
{text_chunk}

---
### Required Output (JSON only)
{{
  "uses_tech": true/false,
  "explanation": "One-sentence justification citing the specific rule or evidence applied",
  "confidence": "high/medium/low"
}}
"""

# ---------- QC Pipeline ----------
def analyze_text(text: str, company: str, keyword: str) -> dict:
    if not text:
        return {"uses_tech": False, "explanation": "No content", "confidence": "low"}

    # Sentiment + entities (extra QC, not main decision)
    doc = nlp(text[:5000])
    entities = [(ent.text, ent.label_) for ent in doc.ents]
    sentiment = sia.polarity_scores(text)

    # Call mistral with strict QC prompt
    prompt = build_prompt(company, keyword, text[:1500])  # limit snippet length
    llm_output = ask_mistral(prompt)

    return {
        "entities": entities,
        "sentiment": sentiment,
        **llm_output
    }

# ---------- Runner ----------
async def run_pipeline(input_csv: str, output_csv: str):
    df = pd.read_csv(input_csv)

    results = []
    for _, row in df.iterrows():
        company = row["Company Name"]
        domain = row["Website"]
        keyword = row["Keyword"]
        url = row["URL"]

        logging.info(f"Processing {company} | {url}")
        html = await fetch_url(url)
        text = extract_clean_text(html)
        analysis = analyze_text(text, company, keyword)

        results.append({
            "company": company,
            "domain": domain,
            "keyword": keyword,
            "url": url,
            **analysis
        })

    out_df = pd.DataFrame(results)
    out_df.to_csv(output_csv, index=False)
    logging.info(f"Results saved to {output_csv}")

# ---------- Main ----------
if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", default="companies.csv", help="Input CSV with company,domain,country,keyword,url")
    parser.add_argument("--output", default="results_mistral.csv", help="Output CSV")
    args = parser.parse_args()

    asyncio.run(run_pipeline(args.input, args.output))
