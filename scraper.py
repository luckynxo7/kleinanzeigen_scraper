"""
scraper.py
--------------

This module implements a simple scraper for the German classifieds website
Kleinanzeigen (formerly eBay Kleinanzeigen).  Given the URL to a seller's
profile page, the scraper iterates over all of that seller's listings,
parses each listing's title and description to extract structured tyre
and wheel information and downloads the associated images.  The end
result is a list of dictionaries ready to be written to a CSV file.

The scraper avoids automated browser drivers and instead relies on the
static HTML that Kleinanzeigen renders on the server.  Each seller
profile page contains a list of listing cards with ``data-href``
attributes.  These relative links point to the individual ad pages.
Pagination is handled by following the ``rel="next"`` link when
present.  Individual listing pages are scraped with plain HTTP
requests.  The description text is then processed with a set of
regular expressions tailored to extract the fields required by the
client application.

Note: This scraper makes HTTP requests to Kleinanzeigen.  When
executing this code you must ensure that you comply with Kleinanzeigen's
terms of service and rate limits.  The user of this code has confirmed
that they have permission from both the platform and the individual
dealers to download their listings.  Nevertheless you should still
introduce delays between requests if scraping large numbers of ads.

"""

from __future__ import annotations

import csv
import os
import re
import time
from dataclasses import dataclass, asdict
from typing import Dict, Iterable, List, Optional, Tuple

import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/122.0.0.0 Safari/537.36"
    )
}


@dataclass
class ListingData:
    """Structured information about a single listing.

    The fields correspond to the import fields on the client's WordPress
    site.  Additional metadata such as the ad URL and title are also
    captured for convenience.
    """

    url: str
    title: str
    felgenhersteller: str = ""
    reifenhersteller: str = ""
    felgenfarbe: str = ""
    zollgroesse: str = ""
    lochkreis: str = ""
    einpresstiefe_vorderachse: str = ""
    einpresstiefe_hinterachse: str = ""
    reifengroesse_vorderachse: str = ""
    reifengroesse_hinterachse: str = ""
    reifenbreite_vorderachse: str = ""
    reifenbreite_hinterachse: str = ""
    nabendurchmesser: str = ""
    reifensaison: str = ""
    profiltiefe_vorderachse: str = ""
    profiltiefe_hinterachse: str = ""
    dot_vorderachse: str = ""
    dot_hinterachse: str = ""
    image_urls: List[str] = None  # will be filled with a list of image URLs

    def as_csv_row(self) -> Dict[str, str]:
        """Return a dictionary suitable for CSV writing."""
        row = asdict(self)
        # Flatten the image list into a semicolon-separated string
        row["image_urls"] = ";".join(self.image_urls or [])
        return row


class KleinanzeigenScraper:
    """Scraper class encapsulating the scraping logic.

    Example usage::

        scraper = KleinanzeigenScraper()
        ad_urls = scraper.scrape_seller('https://www.kleinanzeigen.de/pro/reifenfelgenkeller')
        for ad_url in ad_urls:
            data = scraper.scrape_listing(ad_url)
            print(data)

    """

    def __init__(self, delay: float = 1.0) -> None:
        """Initialise the scraper.

        Parameters
        ----------
        delay : float, optional
            A delay in seconds between successive HTTP requests.  Many
            websites employ rate limiting; a small delay reduces the
            likelihood of being blocked.  Set to zero to disable.
        """
        self.delay = delay

    def _fetch(self, url: str) -> str:
        """Fetch the given URL and return its text content.

        Raises a RuntimeError if the request fails.
        """
        resp = requests.get(url, headers=HEADERS)
        if not resp.ok:
            raise RuntimeError(f"Failed to fetch {url}: {resp.status_code}")
        # optional delay to respect rate limits
        if self.delay:
            time.sleep(self.delay)
        return resp.text

    def scrape_seller(self, seller_url: str) -> List[str]:
        """Return a list of all listing URLs for a given seller.

        This method follows pagination links automatically.  Duplicate
        URLs are removed while preserving order.
        """
        ad_urls: List[str] = []
        seen: set[str] = set()
        next_url: Optional[str] = seller_url
        base = "https://www.kleinanzeigen.de"

        while next_url:
            html = self._fetch(next_url)
            soup = BeautifulSoup(html, "html.parser")
            # find all articles with data-href
            for article in soup.find_all("article", attrs={"data-href": True}):
                rel = article.get("data-href")
                if not rel:
                    continue
                full_url = urljoin(base, rel)
                if full_url not in seen:
                    seen.add(full_url)
                    ad_urls.append(full_url)

            # locate the next page link via rel="next" first
            link = soup.find("a", attrs={"rel": "next"})
            # fallback: look for pagination control labelled 'Weiter' or 'nächste'
            if not link:
                for a in soup.find_all("a"):
                    label = (a.get("aria-label") or a.get_text() or "").lower()
                    if "weiter" in label or "nächste" in label or "next" in label:
                        link = a
                        break
            if link and link.get("href"):
                candidate = urljoin(base, link["href"])
                # break if we would loop forever
                if candidate == next_url:
                    break
                next_url = candidate
            else:
                break

        return ad_urls

    def scrape_listing(self, ad_url: str) -> ListingData:
        """Scrape a single listing page and return a :class:`ListingData` object."""
        html = self._fetch(ad_url)
        soup = BeautifulSoup(html, "html.parser")
        # Extract the title – attempt <h1>, then <title>
        title = ""
        h1 = soup.find(["h1", "h2"], string=True)
        if h1:
            title = h1.get_text(strip=True)
        if not title:
            # fallback to meta or document title
            title_tag = soup.find("title")
            if title_tag:
                title = title_tag.get_text(strip=True)

        # Extract description.  Kleinanzeigen renders the description in
        # various containers; we try a few known selectors and then fall back
        # to all <section> or <div> elements labelled as description.
        description = ""
        possible_selectors = [
            "#viewad-description",
            "#vip-ad-description",
            "div[data-testid='description']",
            "section[data-testid='ad-description']",
        ]
        for sel in possible_selectors:
            element = soup.select_one(sel)
            if element:
                description = element.get_text("\n", strip=True)
                break
        if not description:
            # fallback to the first <section> that contains 'Beschreibung'
            for sec in soup.find_all(["section", "div"]):
                text = sec.get_text("\n", strip=True)
                if "Beschreibung" in text:
                    description = text
                    break

        # Fallback: if still empty, use all text from the page (might be noisy)
        if not description:
            description = soup.get_text("\n", strip=True)

        # Extract image URLs: look for img tags pointing to prod-ads images
        image_urls: List[str] = []
        for img in soup.find_all("img"):
            src = img.get("src") or ""
            if "/api/v1/prod-ads/images/" in src:
                # remove query params to get a consistent URL
                clean = src.split("?")[0]
                if clean not in image_urls:
                    image_urls.append(clean)

        # Parse structured fields from title and description
        data = ListingData(url=ad_url, title=title, image_urls=image_urls)
        self._fill_fields(data, title, description)
        return data

    def _fill_fields(self, data: ListingData, title: str, description: str) -> None:
        """Populate the fields of ``data`` based on the title and description.

        Uses a collection of regular expressions to locate specific
        patterns.  If a pattern matches multiple times we take the
        first occurrence for the front axle and the second for the rear
        axle, where appropriate.
        """
        text = f"{title}\n{description}"

        def first_match(pattern: str, flags: int = re.IGNORECASE) -> Optional[str]:
            m = re.search(pattern, text, flags)
            return m.group(1).strip() if m else None

        def all_matches(pattern: str, flags: int = re.IGNORECASE) -> List[str]:
            return [m.group(1).strip() for m in re.finditer(pattern, text, flags)]

        # Felgenhersteller: usually labelled explicitly, otherwise we try
        # to infer from the title (first word before a model designation)
        fh = first_match(r"Felgenhersteller:?\s*([\wäöüÄÖÜß\-\s]+)")
        if not fh:
            fh = first_match(r"Felgen\s*:\s*([\wäöüÄÖÜß\-\s]+)")
        if not fh:
            # If the title begins with 'Original BMW' or 'BMW', extract 'BMW'
            m = re.match(r"(?:Original\s+)?([A-Z\u00C4-\u00DC][A-Za-z\u00C4-\u00DC\u00E4-\u00FC]+)", title)
            if m:
                fh = m.group(1)
        data.felgenhersteller = fh or ""

        # Reifenhersteller
        rh = first_match(r"Reifenhersteller:?\s*([\wäöüÄÖÜß\-\s]+)")
        if not rh:
            rh = first_match(r"Hersteller:?\s*([\wäöüÄÖÜß\-\s]+)", flags=re.IGNORECASE)
        data.reifenhersteller = rh or ""

        # Felgenfarbe
        ff = first_match(r"Farbe:?\s*([A-Za-zäöüÄÖÜß\-\s]+)")
        if not ff:
            # look for words like 'schwarz', 'silber' etc. near 'Pulverbeschichtung'
            ff = first_match(r"Pulverbeschichtung in der Farbe\s*([A-Za-zäöüÄÖÜß\-\s]+)")
        data.felgenfarbe = ff or ""

        # Zollgröße (wheel diameter)
        zg = first_match(r"Zoll(?:größe)?\s*:?[\s]*(\d{1,2})")
        if not zg:
            # try to extract number before 'Zoll' in title
            zg = first_match(r"(\d{1,2})\s*Zoll")
        data.zollgroesse = zg or ""

        # Lochkreis (PCD)
        lk = first_match(r"Lochkreis:?\s*([\d.,/]+)")
        data.lochkreis = (lk or "").replace(",", ".")

        # Nabendurchmesser (hub diameter)
        nd = first_match(r"(?:Mittenlochbohrung|Nabendurchmesser):?\s*([\d.,]+)")
        data.nabendurchmesser = (nd or "").replace(",", ".")

        # Einpresstiefe (offset) – may have front and rear values
        ets = all_matches(r"Einpresstiefe(?:\s*(?:Vorderachse|Hinterachse))?\s*:?[\s]*(\d{1,3})")
        if ets:
            data.einpresstiefe_vorderachse = ets[0]
            if len(ets) > 1:
                data.einpresstiefe_hinterachse = ets[1]
        # Reifengröße / Maße – may include width/height/rim, separate for front and rear
        sizes = all_matches(r"(?:Reifengröße|Maße)(?:\s*(?:Vorderachse|Hinterachse))?\s*:?[\s]*([\d]{3}/[\d]{2}\s*[Rr]?\s*\d{2})")
        if sizes:
            data.reifengroesse_vorderachse = sizes[0]
            if len(sizes) > 1:
                data.reifengroesse_hinterachse = sizes[1]
        # Derive tyre width from sizes
        def width_from(size: str) -> str:
            if not size:
                return ""
            parts = size.split("/")
            return parts[0].strip() if parts else ""
        data.reifenbreite_vorderachse = width_from(data.reifengroesse_vorderachse)
        data.reifenbreite_hinterachse = width_from(data.reifengroesse_hinterachse)

        # Reifen Saison / Spezifikation
        season = first_match(r"(?:Reifensaison|Spezifikation|Saison):?\s*([A-Za-zäöüÄÖÜß\s]+)")
        data.reifensaison = season or ""

        # Profiltiefe – may be given as 'Vorderachse', 'Hinterachse' or overall
        depths = all_matches(r"Profiltiefe(?:\s*(?:Vorderachse|Hinterachse))?\s*:?[\s]*([\d,.xX\w\s]+)")
        if depths:
            data.profiltiefe_vorderachse = depths[0]
            if len(depths) > 1:
                data.profiltiefe_hinterachse = depths[1]
        # DOT codes – these are four digit year/week codes
        dots = all_matches(r"DOT(?:\s*(?:Vorderachse|Hinterachse))?\s*:?[\s]*([\d\sxX/]+)")
        if dots:
            data.dot_vorderachse = dots[0]
            if len(dots) > 1:
                data.dot_hinterachse = dots[1]

    def save_to_csv(self, listings: Iterable[ListingData], csv_path: str) -> None:
        """Write a list of :class:`ListingData` objects to a CSV file.

        The header row is derived from the field names of ListingData.
        """
        if not listings:
            return
        # Determine header from the dataclass fields
        header = list(listings[0].as_csv_row().keys())
        with open(csv_path, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for item in listings:
                writer.writerow(item.as_csv_row())

    def download_images(self, listing: ListingData, output_dir: str) -> List[str]:
        """Download all images of a listing to the specified directory.

        Returns the list of file paths saved.  Images are stored with
        filenames based on their index in the list and the listing's ad id
        (extracted from the URL).
        """
        os.makedirs(output_dir, exist_ok=True)
        saved: List[str] = []
        ad_id_match = re.search(r"/(\d+)-", listing.url)
        ad_id = ad_id_match.group(1) if ad_id_match else "listing"
        for idx, img_url in enumerate(listing.image_urls or []):
            ext = os.path.splitext(img_url.split("?")[0])[1] or ".jpg"
            filename = f"{ad_id}_{idx+1}{ext}"
            path = os.path.join(output_dir, filename)
            try:
                resp = requests.get(img_url, headers=HEADERS)
                if resp.ok:
                    with open(path, "wb") as f:
                        f.write(resp.content)
                    saved.append(path)
                    if self.delay:
                        time.sleep(self.delay)
            except Exception:
                # ignore individual image download failures
                continue
        return saved
