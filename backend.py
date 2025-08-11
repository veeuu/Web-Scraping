# backend.py
import asyncio
import re
import logging
from urllib.parse import urljoin, urlparse
from io import BytesIO
import datetime as dt

import pandas as pd
import httpx
from playwright.async_api import async_playwright
from bs4 import BeautifulSoup
from pdfminer.high_level import extract_text as pdf_extract_text
from PIL import Image
import pytesseract
import nest_asyncio

from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
from translate import Translator

nest_asyncio.apply()
logging.getLogger("pdfminer").setLevel(logging.ERROR)

model = SentenceTransformer('all-mpnet-base-v2')
translator = Translator(to_lang="en")

# Acronym expansions
ACRONYM_MAP = {
    'aws': 'amazon web services',
    'gcp': 'google cloud platform',
    'azure': 'microsoft azure',
    'SAP': 'Systems, Applications & Products in Data Processing',
    'NSX': 'VMware NSX',
    'ESXi': 'VMware ESXi'
}

# Patterns to detect news/blog sites
news_domains_patterns = [
    r'\bnews\b', r'\bblog\b', r'\brelease\b', r'\barticle\b',
    r'\bpress\b', r'\bmedia\b', r'\bjournal\b', r'\bnewsletter\b'
]

# Patterns to detect course/training sites
course_patterns = [
    r"course", r"training", r"certification", r"academy", r"bootcamp", r"class", r"learn"
]

# Partnership terms prioritized on news/blog sites
PARTNERSHIP_TERMS = [
    "partnership", "collaborate", "collaboration", "joint venture", "alliance", "acquired by",
    "integrates with", "integration", "powered by", "customer", "client", "implements",
    "adopts", "leverages", "agreement", "solution", "product launch", "go-to-market"
]

discussionBase = [
    'blog', 'news', 'report', 'insight', 'article', 'review',
    'compare', 'vs', 'what is', 'how to', 'explore', 'future of',
    'analysis', 'tutorial', 'guide', 'explained', 'trend',
    'research', 'study', 'whitepaper', 'ebook', 'webinar',
    'podcast', 'deep dive', 'breakdown', 'opinion', 'editorial',
    'perspective', 'best practice', 'how it works', 'faq',
    'overview', 'company', 'corporate', 'leadership', 'announcement',
    'press release', 'news release', 'quarterly report', 'annual report',
    'investor relations', 'financial results', 'earnings call',
    'webcast', 'newsletter', 'media coverage', 'public relations', 'stakeholder'
]

hiringBase = [
    'career', 'hiring', 'join our team', 'job', 'apply',
    'opportunity', 'vacancy', 'position', 'role', 'engineer',
    'developer', 'recruitment', 'staffing', 'internship',
    'campus', 'walk in', 'talent acquisition', 'diversity',
    'equal opportunity', 'inclusion', 'culture', 'compensation',
    'salary', 'benefits', 'perks', 'engagement', 'job fair',
    'campus recruitment', 'recruitment drive', 'walk-in interview'
]

usageBase = [
    'partnership', 'partner', 'offering', 'product', 'solution',
    'service', 'platform', 'tool', 'launch', 'introduce',
    'release', 'sell', 'expertise', 'specialize', 'collaboration',
    'alliance', 'joint venture', 'integrate', 'ecosystem',
    'acquisition', 'investment', 'use', 'implement', 'adopt',
    'leverage', 'case study', 'solution brief', 'powered by',
    'built with', 'trusted by', 'api', 'sdk', 'saas', 'paas',
    'iaas', 'cloud native', 'digital transformation',
    'turnkey solution', 'managed service', 'white label',
    'value proposition', 'roi', 'tco', 'kpi', 'compliance',
    'security', 'sla', 'uptime', 'scalability', 'high availability',
    'resiliency', 'cost optimization', 'best in class',
    'plug and play', 'innovative', 'disruptive', 'oem',
    'press kit', 'white paper', 'partner portal'
]

all_terms = set(discussionBase + hiringBase + usageBase + PARTNERSHIP_TERMS)

# --- Helper functions ---

def normalize_company_name(name):
    if not name:
        return ""
    name = name.strip().lower()
    if name.startswith("http"):
        parsed = urlparse(name)
        host = parsed.netloc or parsed.path
    else:
        host = name
    if host.startswith("www."):
        host = host[4:]
    return host

def ensure_https(url):
    url = url.strip()
    if not url.lower().startswith(("http://", "https://")):
        return "https://" + url
    return url

async def translate_if_needed(text):
    try:
        translated_text = translator.translate(text)
        if translated_text.lower() == text.lower():
            return text
        return translated_text
    except Exception as e:
        logging.warning(f"Translation failed: {e}. Returning original text.")
        return text

def contains_whole_word(text, word):
    return re.search(rf'\b{re.escape(word)}\b', text, flags=re.IGNORECASE) is not None

def has_wrong_expansion(text, keyword, correct_expansion):
    if not correct_expansion:
        return False
    text_lower = text.lower()
    if contains_whole_word(text_lower, keyword) and not contains_whole_word(text_lower, correct_expansion):
        if re.search(rf'\b{re.escape(keyword.lower())}\b', text_lower):
            return True
    return False

def is_news_or_course_site(url):
    url_lower = url.lower()
    is_news = any(re.search(p, url_lower) for p in news_domains_patterns)
    is_course = any(re.search(p, url_lower) for p in course_patterns)
    return is_news, is_course

def split_chunks(text, keyword, window_words=400):
    keyword_lower = keyword.lower()
    positions = [m.start() for m in re.finditer(rf'\b{re.escape(keyword_lower)}\b', text.lower())]
    words = re.findall(r'\S+', text)
    chunks = []

    for pos in positions:
        char_count, idx = 0, None
        for i, w in enumerate(words):
            char_count += len(w) + 1
            if char_count >= pos:
                idx = i
                break
        if idx is not None:
            start_idx = max(0, idx - window_words // 2)
            end_idx = min(len(words), idx + window_words // 2 + 1)
            chunk = ' '.join(words[start_idx:end_idx])
            if any(t in chunk.lower() for t in all_terms):
                chunks.append(chunk.strip())

    if not chunks and any(t in text.lower() for t in all_terms):
        chunks.append(text.strip()[:2000])

    return list(set(chunks))

def semantic_filter(chunks, query):
    try:
        if not chunks:
            return []
        chunk_embeddings = model.encode(chunks, show_progress_bar=False)
        query_embedding = model.encode([query], show_progress_bar=False)
        similarities = cosine_similarity(query_embedding, chunk_embeddings)[0]
        return [(chunk, sim) for chunk, sim in zip(chunks, similarities) if sim > 0.1]
    except Exception as e:
        logging.error(f"Semantic filtering failed: {e}")
        return []

def justify_relevance(chunk, company, keyword, score, threshold, is_news=False, is_course=False):
    chunk_lower = chunk.lower()
    matched = [t for t in all_terms if t in chunk_lower]

    usage_matches = [t for t in matched if t in usageBase]
    hiring_matches = [t for t in matched if t in hiringBase]
    discussion_matches = [t for t in matched if t in discussionBase]

    strong_partnership_found = any(term in chunk_lower for term in PARTNERSHIP_TERMS)

    relevance_status = "NOT RELEVANT"
    level = "LOW"
    explanation = "No relevant category terms or strong semantic match found."

    if strong_partnership_found:
        relevance_status = "RELEVANT"
        level = "HIGH"
        explanation = (f"{company} and {keyword} are connected through strong partnership terms: "
                       f"{', '.join(set([t for t in PARTNERSHIP_TERMS if t in chunk_lower]))}.")
    elif usage_matches:
        relevance_status = "RELEVANT"
        level = "HIGH"
        explanation = f"{company} and {keyword} are connected through usage-related terms: {', '.join(set(usage_matches))}."
    elif hiring_matches:
        relevance_status = "RELEVANT"
        level = "MEDIUM"
        explanation = f"{company} and {keyword} are connected through hiring-related terms: {', '.join(set(hiring_matches))}."
    elif discussion_matches:
        if is_news or is_course:
            relevance_status = "NOT RELEVANT"
            level = "LOW"
            explanation = f"Site is news/course related; only discussion terms found, but no strong partnership or usage. {', '.join(set(discussion_matches))}."
        else:
            relevance_status = "RELEVANT"
            level = "LOW"
            explanation = f"{company} and {keyword} are connected through discussion-related terms: {', '.join(set(discussion_matches))}."

    if score >= threshold and relevance_status == "NOT RELEVANT" and not (is_news or is_course):
        relevance_status = "RELEVANT"
        level = "MEDIUM"
        explanation = f"Strong semantic match (score: {score:.2f}) despite weak term matches."

    if (is_news or is_course) and relevance_status == "NOT RELEVANT":
        explanation = f"Site identified as news/course. Original reason: {explanation}"

    return relevance_status, level, explanation

async def fetch_page_content(playwright, url):
    if url.lower().endswith(".pdf"):
        async with httpx.AsyncClient(timeout=20, verify=False) as client:
            try:
                r = await client.get(url)
                r.raise_for_status()
                if r.content[:4] != b"%PDF":
                    return "", "invalid_pdf"
                return pdf_extract_text(BytesIO(r.content)), "pdf"
            except httpx.RequestError as e:
                logging.error(f"HTTP error fetching PDF {url}: {e}")
                return "", "load_failed_http"
            except Exception as e:
                logging.error(f"Error processing PDF {url}: {e}")
                return "", "load_failed_pdf_processing"

    browser = await playwright.chromium.launch()
    page = await browser.new_page()
    try:
        await page.goto(url, timeout=45000)
        await page.wait_for_load_state('networkidle', timeout=30000)
        html = await page.content()
    except Exception as e:
        logging.error(f"Playwright failed to load {url}: {e}")
        html = ""
    finally:
        await browser.close()

    if not html:
        return "", "load_failed_playwright"
    return html, "html"

async def extract_text_from_images(website, keywords):
    ocr_results = {}
    try:
        async with httpx.AsyncClient(timeout=15, verify=False) as client:
            r = await client.get(website)
            r.raise_for_status()
            soup = BeautifulSoup(r.text, 'html.parser')

            img_tasks = []
            for img_tag in soup.find_all("img"):
                img_src = img_tag.get("src")
                if not img_src:
                    continue
                img_url = urljoin(website, img_src)
                img_tasks.append(process_single_image(client, img_url, keywords))

            results = await asyncio.gather(*img_tasks, return_exceptions=True)
            for img_url, found_keywords in results:
                if isinstance(img_url, str) and found_keywords:
                    ocr_results[img_url] = found_keywords
    except httpx.RequestError as e:
        logging.error(f"HTTP error fetching website for image OCR {website}: {e}")
    except Exception as e:
        logging.error(f"Error during image OCR processing for {website}: {e}")
    return ocr_results

async def process_single_image(client, img_url, keywords):
    try:
        img_resp = await client.get(img_url, timeout=10)
        img_resp.raise_for_status()
        img = Image.open(BytesIO(img_resp.content))
        text = pytesseract.image_to_string(img).strip()
        found = [kw for kw in keywords if contains_whole_word(text, kw)]
        if found:
            return img_url, found
    except Exception:
        pass
    return None, None

async def process_row(idx, row, playwright, st=None):
    raw_company = str(row.get('Company Name', '')).strip()
    company = normalize_company_name(raw_company)
    keyword = str(row.get('Technology', '')).strip()
    raw_url = str(row.get('URL', '')).strip()
    url = ensure_https(raw_url)

    if st:
        st.info(f"Processing [{idx+1}]: {url}")

    is_news, is_course = is_news_or_course_site(url)
    html_or_text, content_type = await fetch_page_content(playwright, url)

    if content_type.startswith("load_failed"):
        return [company, url, keyword, content_type, "NOT RELEVANT", "-", "LOW", f"Content loading failed: {content_type.replace('load_failed_', '')}.", "-"]

    if content_type == "invalid_pdf":
        return [company, url, keyword, content_type, "NOT RELEVANT", "-", "LOW", "Invalid PDF file detected.", "-"]

    text = html_or_text if content_type == "pdf" else BeautifulSoup(html_or_text, 'html.parser').get_text(separator=' ', strip=True)
    text = await translate_if_needed(text)

    correct_expansion = ACRONYM_MAP.get(keyword.lower())
    if correct_expansion and has_wrong_expansion(text, keyword, correct_expansion):
        return [company, url, keyword, content_type, "NOT RELEVANT", "-", "LOW", f"Wrong expansion of keyword '{keyword}' found (expected '{correct_expansion}').", "-"]

    ocr_results = await extract_text_from_images(url, [keyword] + list(ACRONYM_MAP.keys()))
    ocr_summary = "; ".join([f"{k}: {', '.join(v)}" for k, v in ocr_results.items()]) if ocr_results else "-"

    chunks = split_chunks(text, keyword)
    if not chunks:
        return [company, url, keyword, content_type, "NOT RELEVANT", "-", "LOW", "No relevant text chunks found around keyword or no general terms.", ocr_summary]

    query = f"What is the relationship between {company} and {keyword}?"
    rel_chunks = semantic_filter(chunks, query)
    if not rel_chunks:
        return [company, url, keyword, content_type, "NOT RELEVANT", "-", "LOW", "No semantically relevant chunks found after filtering.", ocr_summary]

    top_chunk, score = max(rel_chunks, key=lambda x: x[1])
    relevance, level, explanation = justify_relevance(top_chunk, company, keyword, score, threshold=0.4, is_news=is_news, is_course=is_course)

    if ocr_results and relevance == "NOT RELEVANT":
        explanation += f" (Note: OCR detected keywords in images: {ocr_summary})"

    return [company, url, keyword, content_type, relevance, top_chunk, level, explanation, ocr_summary]

async def run_partial_frontend(input_filepath, output_filepath, st=None, single_row=None):
    if single_row is not None:
        df = pd.DataFrame([single_row])
    else:
        df = pd.read_excel(input_filepath)

    results = []
    async with async_playwright() as playwright:
        for idx, row in df.iterrows():
            if st and getattr(st.session_state, "stop_requested", False):
                if st:
                    st.warning("Stop requested. Exiting early.")
                break
            result = await process_row(idx, row, playwright, st)
            results.append(result)

            df_out = pd.DataFrame(results, columns=[
                "Company", "Link", "Keyword", "Content Type",
                "Relevant or Not", "Chunk", "Score Level", "Explanation", "OCR Keywords & Image Links"
            ])
            df_out.to_csv(output_filepath, index=False)

    return output_filepath
