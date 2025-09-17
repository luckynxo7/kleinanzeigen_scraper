"""
app.py
------

Streamlit web application for scraping Kleinanzeigen sellers.

The app allows a user to enter one or more seller profile URLs, then
scrapes every listing associated with those sellers.  It extracts
structured information from each ad and downloads the associated
images.  The results are presented in the browser as a table and can
be downloaded as a CSV file along with a ZIP archive containing all
images.

To run locally::

    streamlit run app.py

Make sure the dependencies listed in requirements.txt are installed
(e.g. via ``pip install -r requirements.txt``).

"""

from __future__ import annotations

import io
import os
import zipfile
from typing import List

import pandas as pd
import streamlit as st

from scraper import KleinanzeigenScraper, ListingData


def main() -> None:
    st.set_page_config(page_title="Kleinanzeigen Scraper", layout="wide")
    st.title("Kleinanzeigen Händler-Scraper")
    st.markdown(
        """
        Geben Sie unten die URLs der Händlerprofile ein (eine URL pro Zeile).
        Das Tool lädt alle Anzeigen dieser Händler, extrahiert relevante
        Informationen und sammelt die Bilder.  Nachdem der Vorgang
        abgeschlossen ist, können Sie die Daten als CSV herunterladen
        sowie ein ZIP-Archiv der Bilder.
        """
    )

    # Input for seller URLs
    sellers_input = st.text_area(
        "Händler URLs (eine pro Zeile)",
        placeholder="https://www.kleinanzeigen.de/pro/reifenfelgenkeller\nhttps://www.kleinanzeigen.de/pro/andererhaendler",
        height=150,
    )
    delay = st.number_input(
        "Verzögerung zwischen Requests (Sekunden)", value=1.0, min_value=0.0, max_value=10.0, step=0.5
    )
    start_button = st.button("Scrape starten")

    if start_button and sellers_input.strip():
        seller_urls = [line.strip() for line in sellers_input.splitlines() if line.strip()]
        scraper = KleinanzeigenScraper(delay=delay)
        all_listings: List[ListingData] = []
        image_files: List[str] = []
        output_images_dir = "downloaded_images"
        if os.path.exists(output_images_dir):
            # Clean up from previous runs
            for root, _dirs, files in os.walk(output_images_dir):
                for f in files:
                    os.remove(os.path.join(root, f))
        else:
            os.makedirs(output_images_dir, exist_ok=True)

        # Progress bar
        progress_text = st.empty()
        progress_bar = st.progress(0.0)
        total_sellers = len(seller_urls)
        processed_sellers = 0

        for seller in seller_urls:
            processed_sellers += 1
            progress_text.write(f"Verarbeite Händler {processed_sellers}/{total_sellers}: {seller}")
            try:
                ad_urls = scraper.scrape_seller(seller)
            except Exception as e:
                st.error(f"Fehler beim Abrufen der Händlerseite {seller}: {e}")
                continue

            total_ads = len(ad_urls)
            for idx, ad_url in enumerate(ad_urls, start=1):
                progress_bar.progress(min(1.0, (processed_sellers - 1 + idx / max(total_ads, 1)) / total_sellers))
                try:
                    listing = scraper.scrape_listing(ad_url)
                    all_listings.append(listing)
                    # Download images for this listing
                    saved = scraper.download_images(listing, output_images_dir)
                    image_files.extend(saved)
                except Exception as e:
                    st.warning(f"Fehler beim Scrapen der Anzeige {ad_url}: {e}")
                    continue

        progress_bar.progress(1.0)
        progress_text.write("Fertig!")

        if all_listings:
            # Create DataFrame
            df = pd.DataFrame([lst.as_csv_row() for lst in all_listings])
            st.subheader("Vorschau der Daten")
            st.dataframe(df)

            # Prepare CSV for download
            csv_buffer = io.StringIO()
            df.to_csv(csv_buffer, index=False)
            csv_bytes = csv_buffer.getvalue().encode("utf-8")

            st.download_button(
                label="CSV herunterladen",
                data=csv_bytes,
                file_name="kleinanzeigen_daten.csv",
                mime="text/csv",
            )

            # Prepare ZIP of images
            if image_files:
                zip_buffer = io.BytesIO()
                with zipfile.ZipFile(zip_buffer, mode="w", compression=zipfile.ZIP_DEFLATED) as zf:
                    for file_path in image_files:
                        arcname = os.path.relpath(file_path, output_images_dir)
                        zf.write(file_path, arcname)
                zip_buffer.seek(0)
                st.download_button(
                    label="Bilder als ZIP herunterladen",
                    data=zip_buffer,
                    file_name="kleinanzeigen_bilder.zip",
                    mime="application/zip",
                )
            else:
                st.info("Keine Bilder zum Herunterladen vorhanden.")
        else:
            st.info("Es wurden keine Anzeigen gefunden.")


if __name__ == "__main__":
    main()
