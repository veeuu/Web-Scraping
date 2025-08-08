import streamlit as st
import asyncio
from pathlib import Path
from backend_scrapingdog import main as scrapingdog_main

st.set_page_config(page_title="🔎 ScrapingDog Scraper Dashboard", layout="wide")
st.title("🔎 ScrapingDog Web Scraper Dashboard")

st.sidebar.header("Upload Inputs & API Key")

uploaded_companies_file = st.sidebar.file_uploader("Upload Input File (CSV or XLSX)", type=["csv", "xlsx"])
uploaded_keywords_file = st.sidebar.file_uploader("Upload Keywords JSON File", type=["json"])
api_key = st.sidebar.text_input("Enter ScrapingDog API Key", type="password")

status_text = st.empty()
progress_bar = st.progress(0)

def progress_callback(message):
    status_text.text(message)

def run_scraper(input_path, keywords_path, api_key):
    return asyncio.run(scrapingdog_main(input_path, keywords_path, api_key, progress_callback=progress_callback))

if uploaded_companies_file and uploaded_keywords_file and api_key:
    input_path = Path("input_companies" + (".csv" if uploaded_companies_file.type == "text/csv" else ".xlsx"))
    keywords_path = Path("keywords.json")

    with open(input_path, "wb") as f:
        f.write(uploaded_companies_file.getbuffer())

    with open(keywords_path, "wb") as f:
        f.write(uploaded_keywords_file.getbuffer())

    if st.button("🚀 Start Scraping"):
        with st.spinner("Running ScrapingDog scraper... This may take a while ⏳"):
            try:
                output_csv = run_scraper(str(input_path), str(keywords_path), api_key)

                st.success("✅ Scraping completed!")

                if output_csv and Path(output_csv).exists():
                    with open(output_csv, "rb") as f:
                        st.download_button(
                            label="📥 Download Output CSV",
                            data=f,
                            file_name=Path(output_csv).name,
                            mime="text/csv"
                        )
                else:
                    st.error("❌ Output CSV not found or scraper failed.")
            except Exception as e:
                st.error(f"❌ Error running scraper: {e}")
else:
    st.info("Please upload input file, keywords JSON, and enter your ScrapingDog API key to start.")