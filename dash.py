import streamlit as st
import pandas as pd
import asyncio
import os
import time
import platform
import warnings
import logging
from backend import run_partial_frontend
import nest_asyncio
nest_asyncio.apply()

if platform.system() == "Windows":
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())

warnings.filterwarnings("ignore")
logging.getLogger('streamlit.runtime.scriptrunner.script_runner').setLevel(logging.ERROR)

st.set_page_config(page_title="🚀 Smart Web Scraping Dashboard", layout="wide")

st.title("🕸️ Smart Web Scraping & Relevance Detection Dashboard")

if "stop_requested" not in st.session_state:
    st.session_state.stop_requested = False

with st.sidebar:
    st.header("📂 Upload Excel")
    uploaded_file = st.file_uploader("Upload file (.xlsx)", type=["xlsx"])
    st.markdown("---")
    st.info("This tool scrapes webpages, extracts text/images, and checks if a company is **relevant** to a given technology using semantic filtering.")

if uploaded_file:
    st.success("✅ File uploaded successfully!")
    df = pd.read_excel(uploaded_file)

    preview_placeholder = st.empty()
    with preview_placeholder.container():
        st.subheader("📊 Preview of Uploaded Data")
        st.dataframe(df.head(), use_container_width=True)

    col1, col2 = st.columns(2)
    with col1:
        run_button = st.button("🚀 Start Analysis")
    with col2:
        stop_button = st.button("🛑 Stop")

    if run_button:
        st.session_state.stop_requested = False


        file_path = "uploaded_input.xlsx"
        with open(file_path, "wb") as f:
            f.write(uploaded_file.getbuffer())


        status_placeholder = st.empty()
        live_table_placeholder = st.empty()

        import nest_asyncio
        nest_asyncio.apply()
        import asyncio

        async def run_with_highlight():
            df_copy = df.copy()
            results = []
            latest_checkpoint_path = None
            output_path = "output.csv"

            for idx, row in df.iterrows():
                if st.session_state.stop_requested:
                    status_placeholder.warning("🛑 Stopped by user")
                    break

                def highlight_row(x):
                    return ['background-color: #add8e6' if i == idx else '' for i in range(len(x))]

                styled_df = df_copy.style.apply(highlight_row, axis=0)
                live_table_placeholder.dataframe(styled_df, use_container_width=True)

                status_placeholder.info(f"🔄 Processing URL: {row['URL']}")

                try:
                    latest_checkpoint_path = await run_partial_frontend(file_path, output_path, st, single_row=row)
                    results.append(latest_checkpoint_path)
                except Exception as e:
                    status_placeholder.error(f"Error processing row {idx}: {e}")
                    break

            return latest_checkpoint_path, output_path

        loop = asyncio.get_event_loop()
        latest_checkpoint_path, output_path = loop.run_until_complete(run_with_highlight())

        st.success("✅ Completed processing")


        if latest_checkpoint_path and os.path.exists(latest_checkpoint_path):
            with open(latest_checkpoint_path, "rb") as f:
                st.download_button(
                    label="📥 Download Last Checkpoint File",
                    data=f,
                    file_name=os.path.basename(latest_checkpoint_path),
                    mime="text/csv"
                )

        # Check for final output file
        if os.path.exists(output_path):
            with open(output_path, "rb") as f:
                st.download_button(
                    label="📥 Download Final Output File",
                    data=f,
                    file_name="output.csv",
                    mime="text/csv"
                )

    elif stop_button:
        st.session_state.stop_requested = True
        st.warning("🛑 Stop requested. Partial results will be saved.")

else:
    st.warning("👈 Please upload a file to get started.")
