import argparse
import asyncio
import logging
import re
from io import BytesIO
from urllib.parse import urljoin, urlparse
import sys
import os
import pandas as pd
import httpx
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text
from PIL import Image
import pytesseract
import trafilatura
from sentence_transformers import SentenceTransformer
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.naive_bayes import MultinomialNB
import numpy as np
import spacy
import nltk
from nltk.sentiment import SentimentIntensityAnalyzer

try:
    from translate import Translator
    translator = Translator(to_lang="en")
    TRANSLATOR_AVAILABLE = True
except Exception:
    translator = None
    TRANSLATOR_AVAILABLE = False


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
logger = logging.getLogger(__name__)

# NLP / Models (lazy-loaded)
LOGGER = logger
MODEL = None
NLP = None
SIA = None
VECTORIZER = None
CLASSIFIER = None

# Acronym expansions (example)
ACRONYM_MAP = {
    'aws': 'amazon web services',
    'gcp': 'google cloud platform',
    'azure': 'microsoft azure',
    'sap': 'systems, applications & products in data processing',
}

# Terms for filtering (you can expand)
PARTNERSHIP_TERMS = [
    "partnership", "collaborate", "collaboration", "joint venture", "alliance",
    "integrates with", "integration", "powered by", "customer", "client", "implements",
    "adopts", "leverages", "agreement", "solution", "product launch"
]

# Combined useful terms
discussionBase = [
    'blog', 'news', 'report', 'insight', 'article', 'review', 'compare', 'vs',
    'what is', 'how to', 'explore', 'analysis', 'tutorial', 'guide',
]
hiringBase = [
    'career', 'hiring', 'job', 'apply', 'vacancy', 'position', 'role', 'internship'
]
usageBase = [
    'partnership', 'partner', 'offering', 'product', 'solution', 'service',
    'launch', 'integrate', 'api', 'sdk', 'saas', 'paas', 'iaas', 'case study'
]
ALL_TERMS = set(discussionBase + hiringBase + usageBase + PARTNERSHIP_TERMS)

# utility functions

def normalize_company_name(name: str) -> str:
    if not name:
        return ""
    name = str(name).strip()
    if name.lower().startswith("http"):
        parsed = urlparse(name)
        host = parsed.netloc or parsed.path
    else:
        host = name
    host = host.strip().lower()
    if host.startswith("www."):
        host = host[4:]
    return host

def ensure_https(url: str) -> str:
    if not url:
        return url
    url = url.strip()
    if not url.lower().startswith(("http://", "https://")):
        return "https://" + url
    return url

def contains_whole_word(text: str, word: str) -> bool:
    if not text or not word:
        return False
    return re.search(rf'\b{re.escape(word)}\b', text, flags=re.IGNORECASE) is not None

async def translate_if_needed(text: str) -> str:
    if not TRANSLATOR_AVAILABLE:
        return text
    try:
        translated = translator.translate(text)
        # translator may return same text if already english
        if translated and translated.strip():
            return translated
        return text
    except Exception as e:
        logger.warning(f"Translation failed: {e}")
        return text

def is_news_or_course_site(url: str):
    lower = url.lower() if url else ""
    is_news = bool(re.search(r'\b(news|blog|press|media|journal|release|article)\b', lower))
    is_course = bool(re.search(r'\b(course|training|academy|bootcamp|class|learn)\b', lower))
    return is_news, is_course

def split_chunks(text: str, keyword: str, window_words: int = 400):
    """
    Return list of chunks (strings) centered around occurrences of keyword.
    If no keyword occurrences but overall text has any of ALL_TERMS, return a truncated chunk.
    """
    if not text:
        return []
    text_lower = text.lower()
    positions = [m.start() for m in re.finditer(rf'\b{re.escape(keyword.lower())}\b', text_lower)] if keyword else []
    words = re.findall(r'\S+', text)
    if positions:
        chunks = []
        for pos in positions:
            # find approximate word index for char pos
            char_count = 0
            idx = None
            for i, w in enumerate(words):
                char_count += len(w) + 1
                if char_count >= pos:
                    idx = i
                    break
            if idx is None:
                continue
            start_idx = max(0, idx - window_words // 2)
            end_idx = min(len(words), idx + window_words // 2 + 1)
            chunk = ' '.join(words[start_idx:end_idx])
            if any(t in chunk.lower() for t in ALL_TERMS):
                chunks.append(chunk.strip())
        return list(dict.fromkeys(chunks))
    else:
        if any(t in text_lower for t in ALL_TERMS):
            return [text.strip()[:2000]]
    return []

def semantic_filter(chunks, query, model, threshold=0.1):
    """Return list of (chunk, similarity) with sim > threshold."""
    try:
        if not chunks:
            return []
        chunk_embeddings = model.encode(chunks, show_progress_bar=False)
        query_embedding = model.encode([query], show_progress_bar=False)
        # compute cosine similarity
        from sklearn.metrics.pairwise import cosine_similarity
        sims = cosine_similarity(query_embedding, chunk_embeddings)[0]
        return [(chunk, float(sim)) for chunk, sim in zip(chunks, sims) if sim > threshold]
    except Exception as e:
        logger.error(f"semantic_filter error: {e}")
        return []

def justify_relevance(chunk, company, keyword, score, threshold, is_news=False, is_course=False):
    chunk_lower = chunk.lower()
    matched = [t for t in ALL_TERMS if t in chunk_lower]
    usage_matches = [t for t in matched if t in usageBase]
    hiring_matches = [t for t in matched if t in hiringBase]
    discussion_matches = [t for t in matched if t in discussionBase]
    strong_partnership = any(term in chunk_lower for term in PARTNERSHIP_TERMS)

    relevance_status = "NOT RELEVANT"
    level = "LOW"
    explanation = "No strong matches."

    if strong_partnership:
        relevance_status = "RELEVANT"
        level = "HIGH"
        explanation = f"Found partnership terms: {', '.join(set([t for t in PARTNERSHIP_TERMS if t in chunk_lower]))}."
    elif usage_matches:
        relevance_status = "RELEVANT"
        level = "HIGH"
        explanation = f"Usage-related terms present: {', '.join(set(usage_matches))}."
    elif hiring_matches:
        relevance_status = "RELEVANT"
        level = "MEDIUM"
        explanation = f"Hiring/Recruitment terms present: {', '.join(set(hiring_matches))}."
    elif discussion_matches:
        if is_news or is_course:
            relevance_status = "NOT RELEVANT"
            level = "LOW"
            explanation = f"Only discussion terms on a news/course site: {', '.join(set(discussion_matches))}."
        else:
            relevance_status = "RELEVANT"
            level = "LOW"
            explanation = f"Discussion-related terms: {', '.join(set(discussion_matches))}."

    if score >= threshold and relevance_status == "NOT RELEVANT" and not (is_news or is_course):
        relevance_status = "RELEVANT"
        level = "MEDIUM"
        explanation = f"Semantic similarity strong (score={score:.2f}) despite limited explicit keywords."

    if (is_news or is_course) and relevance_status == "NOT RELEVANT":
        explanation = f"Site identified as news/course. {explanation}"

    return relevance_status, level, explanation

# Fetch page content (pdf or html) using Playwright for JS-rendered pages
async def fetch_page_content(playwright, url: str):
    url = ensure_https(url)
    if url.lower().endswith(".pdf"):
        # fetch via httpx (no JS)
        try:
            async with httpx.AsyncClient(timeout=30, verify=False) as client:
                r = await client.get(url)
                r.raise_for_status()
                content = r.content
                if not content[:4] == b"%PDF":
                    return "", "invalid_pdf"
                # use pdfminer
                try:
                    text = pdf_extract_text(BytesIO(content))
                    return text, "pdf"
                except Exception as e:
                    logger.error(f"PDF extract error: {e}")
                    return "", "load_failed_pdf_processing"
        except httpx.RequestError as e:
            logger.error(f"HTTP error fetching PDF {url}: {e}")
            return "", "load_failed_http"
    # for html pages use playwright
    browser = await playwright.chromium.launch()
    page = await browser.new_page()
    try:
        await page.goto(url, timeout=45000)
        await page.wait_for_load_state('networkidle', timeout=30000)
        html = await page.content()
        await browser.close()
        return html, "html"
    except Exception as e:
        logger.error(f"Playwright failed to load {url}: {e}")
        try:
            await browser.close()
        except Exception:
            pass
        return "", "load_failed_playwright"

# Extract text using trafilatura (preferred) with fallback to BeautifulSoup
def clean_text_from_html(html_content: str):
    if not html_content:
        return ""
    try:
        extracted = trafilatura.extract(html_content, include_comments=False, include_tables=False)
        if extracted and extracted.strip():
            return extracted
    except Exception as e:
        logger.debug(f"trafilatura extraction failed: {e}")
    # fallback
    try:
        soup = BeautifulSoup(html_content, 'html.parser')
        text = soup.get_text(separator=' ', strip=True)
        return text
    except Exception as e:
        logger.error(f"BeautifulSoup fallback failed: {e}")
        return ""

# OCR images on page to find keywords
async def extract_text_from_images(website_url, keywords):
    results = {}
    try:
        async with httpx.AsyncClient(timeout=20, verify=False) as client:
            r = await client.get(website_url)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')
            img_tasks = []
            image_urls = []
            for img_tag in soup.find_all('img'):
                src = img_tag.get('src')
                if not src:
                    continue
                img_url = urljoin(website_url, src)
                image_urls.append(img_url)
            # process synchronously to keep code simple
            for img_url in image_urls:
                try:
                    resp = await client.get(img_url, timeout=15)
                    resp.raise_for_status()
                    img = Image.open(BytesIO(resp.content)).convert('RGB')
                    text = pytesseract.image_to_string(img).strip()
                    found = [kw for kw in keywords if contains_whole_word(text, kw)]
                    if found:
                        results[img_url] = found
                except Exception:
                    continue
    except Exception as e:
        logger.debug(f"Image OCR overall failed: {e}")
    return results

# Initialize heavy models (lazy)
def init_models():
    global MODEL, NLP, SIA, VECTORIZER, CLASSIFIER
    if MODEL is None:
        logger.info("Loading embedding model (sentence-transformers/all-mpnet-base-v2)...")
        MODEL = SentenceTransformer('all-mpnet-base-v2')
    if NLP is None:
        logger.info("Loading spaCy model (en_core_web_sm)...")
        NLP = spacy.load("en_core_web_sm")
    if SIA is None:
        logger.info("Loading NLTK VADER...")
        try:
            nltk.data.find('sentiment/vader_lexicon.zip')
        except LookupError:
            nltk.download('vader_lexicon')
        SIA = SentimentIntensityAnalyzer()
    if VECTORIZER is None or CLASSIFIER is None:
        # Quick seed classifier - replace with proper training if available
        logger.info("Preparing quick TF-IDF + MultinomialNB classifier (seed examples).")
        seed_docs = [
            "Company X partners with AWS to offer cloud solutions",
            "We are hiring software engineers and devops",
            "Read our latest blog about productivity and best practices",
            "Company Y integrates with Google Cloud Platform for storage",
            "Join our team - open positions in marketing and engineering",
            "Announcement: new product launch and solution brief"
        ]
        seed_labels = ["partnership", "hiring", "blog", "partnership", "hiring", "partnership"]
        VECTORIZER = TfidfVectorizer(max_features=5000, stop_words='english')
        X = VECTORIZER.fit_transform(seed_docs)
        CLASSIFIER = MultinomialNB()
        CLASSIFIER.fit(X, seed_labels)

# Entity extraction helper
def extract_entities(text):
    if not NLP or not text:
        return []
    doc = NLP(text)
    ents = [(ent.text, ent.label_) for ent in doc.ents if ent.label_ in ("ORG", "PRODUCT", "GPE", "PERSON")]
    # return only unique entity texts
    return list(dict.fromkeys([t for t, _ in ents]))

# Simple classification wrapper
def classify_text_category(text):
    if not VECTORIZER or not CLASSIFIER or not text:
        return "-"
    try:
        X = VECTORIZER.transform([text])
        return CLASSIFIER.predict(X)[0]
    except Exception as e:
        logger.debug(f"classify_text_category error: {e}")
        return "-"

# main row processing
async def process_row(idx, row, playwright, threshold=0.4):
    company_raw = row.get('Company Name') or row.get('company') or row.get('Company') or ""
    company = normalize_company_name(company_raw)
    keyword = str(row.get('Keyword') or row.get('Technology') or row.get('keyword') or "").strip()
    raw_url = str(row.get('URL') or row.get('Link') or row.get('link') or "")
    url = ensure_https(raw_url)

    is_news, is_course = is_news_or_course_site(url)
    html_or_text, content_type = await fetch_page_content(playwright, url)

    if content_type.startswith("load_failed"):
        return {
            "Company": company,
            "Link": url,
            "Keyword": keyword,
            "Content Type": content_type,
            "Relevant or Not": "NOT RELEVANT",
            "Chunk": "-",
            "Score Level": "LOW",
            "Explanation": f"Content loading failed: {content_type.replace('load_failed_','')}",
            "OCR Keywords & Image Links": "-",
            "Predicted Category": "-",
            "Entities Found": "-",
            "Sentiment": "-",
            "Load Status": content_type
        }

    if content_type == "invalid_pdf":
        return {
            "Company": company,
            "Link": url,
            "Keyword": keyword,
            "Content Type": "invalid_pdf",
            "Relevant or Not": "NOT RELEVANT",
            "Chunk": "-",
            "Score Level": "LOW",
            "Explanation": "Invalid PDF",
            "OCR Keywords & Image Links": "-",
            "Predicted Category": "-",
            "Entities Found": "-",
            "Sentiment": "-",
            "Load Status": "invalid_pdf"
        }

    # Extract clean text
    if content_type == "pdf":
        text = html_or_text
    else:
        text = clean_text_from_html(html_or_text)

    # translate if needed (optional)
    try:
        text = await translate_if_needed(text)
    except Exception:
        pass

    # OCR images for keywords and acronyms
    ocr_keywords = await extract_text_from_images(url, [keyword] + list(ACRONYM_MAP.keys())) if keyword else {}
    ocr_summary = "; ".join([f"{k}: {', '.join(v)}" for k, v in ocr_keywords.items()]) if ocr_keywords else "-"

    # early filter: text size
    if not text or len(text.strip()) < 100:
        return {
            "Company": company,
            "Link": url,
            "Keyword": keyword,
            "Content Type": content_type,
            "Relevant or Not": "NOT RELEVANT",
            "Chunk": "-",
            "Score Level": "LOW",
            "Explanation": "Extracted text too short or empty.",
            "OCR Keywords & Image Links": ocr_summary,
            "Predicted Category": "-",
            "Entities Found": "-",
            "Sentiment": "-",
            "Load Status": content_type
        }

    # check wrong acronym expansion
    correct_expansion = ACRONYM_MAP.get(keyword.lower()) if keyword else None
    if correct_expansion:
        # if keyword exists but expansion not present and page seems to use wrong expansion, flag
        if contains_whole_word(text, keyword) and not contains_whole_word(text, correct_expansion):
            return {
                "Company": company,
                "Link": url,
                "Keyword": keyword,
                "Content Type": content_type,
                "Relevant or Not": "NOT RELEVANT",
                "Chunk": "-",
                "Score Level": "LOW",
                "Explanation": f"Keyword '{keyword}' appears but correct expansion '{correct_expansion}' not present (possible false context).",
                "OCR Keywords & Image Links": ocr_summary,
                "Predicted Category": "-",
                "Entities Found": "-",
                "Sentiment": "-",
                "Load Status": content_type
            }

    # split chunks around keyword
    chunks = split_chunks(text, keyword)
    if not chunks:
        return {
            "Company": company,
            "Link": url,
            "Keyword": keyword,
            "Content Type": content_type,
            "Relevant or Not": "NOT RELEVANT",
            "Chunk": "-",
            "Score Level": "LOW",
            "Explanation": "No relevant chunks found around keyword or no general terms.",
            "OCR Keywords & Image Links": ocr_summary,
            "Predicted Category": "-",
            "Entities Found": "-",
            "Sentiment": "-",
            "Load Status": content_type
        }

    # semantic filter using embeddings
    query = f"What is the relationship between {company} and {keyword}?"
    rel_chunks = semantic_filter(chunks, query, MODEL, threshold=0.05)  # small prefilter threshold
    if not rel_chunks:
        # fallback: take chunks and still run justification based on terms
        top_chunk = chunks[0]
        score = 0.0
    else:
        top_chunk, score = max(rel_chunks, key=lambda x: x[1])

    # compute entities and sentiment and predicted category
    entities = extract_entities(top_chunk)
    sentiment_scores = SIA.polarity_scores(top_chunk) if SIA else {}
    sentiment_summary = f"neg:{sentiment_scores.get('neg',0):.2f}, neu:{sentiment_scores.get('neu',0):.2f}, pos:{sentiment_scores.get('pos',0):.2f}, comp:{sentiment_scores.get('compound',0):.2f}" if sentiment_scores else "-"
    predicted_category = classify_text_category(top_chunk)

    relevance, level, explanation = justify_relevance(top_chunk, company, keyword, score, threshold, is_news=is_news, is_course=is_course)

    # if no entities found but OCR found keywords, consider note
    if relevance == "NOT RELEVANT" and ocr_keywords:
        explanation += f" (OCR detected keywords in images: {ocr_summary})"

    # final packaging
    return {
        "Company": company,
        "Link": url,
        "Keyword": keyword,
        "Content Type": content_type,
        "Relevant or Not": relevance,
        "Chunk": top_chunk[:4000] if top_chunk else "-",
        "Score Level": level,
        "Explanation": explanation,
        "OCR Keywords & Image Links": ocr_summary,
        "Predicted Category": predicted_category,
        "Entities Found": ", ".join(entities) if entities else "-",
        "Sentiment": sentiment_summary,
        "Load Status": content_type
    }

async def run_pipeline(input_path, output_path):
    # read input
    if input_path.lower().endswith(('.xls', '.xlsx')):
        df = pd.read_excel(input_path)
    else:
        df = pd.read_csv(input_path)
    # normalize columns to expected
    df = df.rename(columns={c: c.strip() for c in df.columns})
    init_models()
    results = []
    async with async_playwright() as playwright:
        for idx, row in df.iterrows():
            try:
                logger.info(f"Processing row {idx+1}/{len(df)}")
                res = await process_row(idx, row, playwright, threshold=0.4)
            except Exception as e:
                logger.exception(f"Error processing row {idx}: {e}")
                res = {
                    "Company": normalize_company_name(row.get('Company Name') or row.get('company') or ""),
                    "Link": ensure_https(str(row.get('URL') or row.get('Link') or "")),
                    "Keyword": str(row.get('Keyword') or ""),
                    "Content Type": "error",
                    "Relevant or Not": "NOT RELEVANT",
                    "Chunk": "-",
                    "Score Level": "LOW",
                    "Explanation": f"Processing exception: {e}",
                    "OCR Keywords & Image Links": "-",
                    "Predicted Category": "-",
                    "Entities Found": "-",
                    "Sentiment": "-",
                    "Load Status": "error"
                }
            results.append(res)
            # Save incremental output after each row
            out_df = pd.DataFrame(results)
            out_df.to_csv(output_path, index=False)
    logger.info(f"Completed. Results written to {output_path}")
    return output_path

def main():
    parser = argparse.ArgumentParser(description="QC Scraper - semantic + NLP QC for scraped pages")
    parser.add_argument("input", help="Input file path (.xlsx or .csv)")
    parser.add_argument("output", help="Output CSV path")
    args = parser.parse_args()
    input_path = args.input
    output_path = args.output
    # create output dir if needed
    out_dir = os.path.dirname(os.path.abspath(output_path))
    if out_dir and not os.path.exists(out_dir):
        os.makedirs(out_dir, exist_ok=True)

    # run asyncio event loop
    try:
        asyncio.run(run_pipeline(input_path, output_path))
    except KeyboardInterrupt:
        logger.warning("Interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.exception(f"Fatal error: {e}")
        sys.exit(2)

if __name__ == "__main__":
    main()
