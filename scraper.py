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

# Build default request headers.  We support overriding the
# User‑Agent via an environment variable (KLEINANZEIGEN_UA).  When
# provided, that value will replace the default browser signature.  A
# cookie header can also be provided via KLEINANZEIGEN_COOKIE.  See
# the ``__init__`` method for how these values are applied to the
# session used for all requests.
HEADERS = {
    "User-Agent": os.getenv(
        "KLEINANZEIGEN_UA",
        (
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        ),
    ),
    # Accept & language headers mimic a real browser.  These help avoid
    # 403 responses when Kleinanzeigen performs bot detection.  They can
    # be overridden by the caller if necessary by updating the session.
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "de-DE,de;q=0.9,en;q=0.8",
    "Upgrade-Insecure-Requests": "1",
    "Connection": "keep-alive",
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
        # Use a session so that cookies and headers persist across
        # requests.  This improves performance and allows us to send
        # authentication/consent cookies to Kleinanzeigen.  If the user
        # defines KLEINANZEIGEN_COOKIE (copied from their browser), it
        # will be attached to every request.
        self.session = requests.Session()
        # Apply default headers to the session.  Individual calls may
        # override or extend these via the headers argument on
        # ``session.get``.
        self.session.headers.update(HEADERS)
        # If the user provides a cookie string via environment variable,
        # attach it.  Kleinanzeigen uses this cookie to determine
        # consent and personalise results.  Without it many requests
        # return a 403 or show a consent page.
        cookie = os.getenv("KLEINANZEIGEN_COOKIE")
        if cookie:
            self.session.headers["Cookie"] = cookie
        # Perform a warm‑up request to establish any additional session
        # cookies (e.g. load balancer cookies).  We ignore failures
        # here; they will be surfaced on subsequent real requests.
        try:
            self.session.get("https://www.kleinanzeigen.de", timeout=20, allow_redirects=True)
        except requests.RequestException:
            pass

    def _fetch(self, url: str, referer: Optional[str] = None) -> str:
        """Fetch the given URL and return its text content.

        A ``Referer`` header can be provided to better mimic browser
        navigation.  Any session‑wide cookies and headers are
        automatically included.  Raises a RuntimeError if the request
        fails.
        """
        headers: Dict[str, str] = {}
        if referer:
            headers["Referer"] = referer
        try:
            resp = self.session.get(url, headers=headers, timeout=20, allow_redirects=True)
        except requests.RequestException as exc:
            raise RuntimeError(f"Failed to fetch {url}: {exc}") from exc
        if not resp.ok:
            raise RuntimeError(f"Failed to fetch {url}: {resp.status_code}")
        # optional delay to respect rate limits
        if self.delay:
            time.sleep(self.delay)
        return resp.text

    def scrape_seller(self, seller_url: str) -> List[str]:
        """Return a list of all listing URLs for a given seller.

        The ``seller_url`` can be either a seller profile (``/pro/...``)
        or a pre‑constructed inventory URL (``/s-bestandsliste.html?userId=...``).
        This method attempts to collect all ad URLs from the given
        page.  If the page does not contain any ad items or appears to
        show only a subset, it will try to derive the seller's
        ``userId`` and load the full inventory list.  Duplicate URLs
        are removed while preserving order.
        """
        seen: set[str] = set()
        ad_urls: List[str] = []

        def collect_from_html(html: str, base_url: str) -> None:
            """Internal helper to collect ad URLs from a piece of HTML.

            It populates ``ad_urls`` and ``seen`` in the enclosing scope.
            """
            soup = BeautifulSoup(html, "html.parser")
            # Primary: <article data-href="..."> elements
            for article in soup.find_all("article", attrs={"data-href": True}):
                rel = article.get("data-href")
                if not rel:
                    continue
                full = urljoin(base_url, rel)
                if full not in seen:
                    seen.add(full)
                    ad_urls.append(full)
            # Fallback: <a href="/s-anzeige/..."> links outside of articles
            for a in soup.find_all("a", href=True):
                href = a["href"]
                if href.startswith("/s-anzeige/"):
                    full = urljoin(base_url, href)
                    if full not in seen:
                        seen.add(full)
                        ad_urls.append(full)
            return

        def extract_user_id(html: str) -> Optional[str]:
            """Attempt to extract a seller userId from the page source.

            Kleinanzeigen embeds a user identifier in several places on
            seller pages and ad pages.  This helper searches for
            patterns like ``userId: 12345``, ``sellerId=12345`` or
            ``memberId": "12345"`` within JSON or markup.  Returns the
            first ID found or ``None`` if none match.
            """
            patterns = [
                r"userId[\"']?\s*[:=]\s*[\"']?(\d+)",
                r"sellerId[\"']?\s*[:=]\s*[\"']?(\d+)",
                r"memberId[\"']?\s*[:=]\s*[\"']?(\d+)",
                r"\"userId\"\s*:\s*\"?(\d+)\"?",
            ]
            for pat in patterns:
                m = re.search(pat, html, re.IGNORECASE)
                if m:
                    return m.group(1)
            return None

        # Step 1: fetch the initial page and collect any ads present
        base_url = seller_url
        html = self._fetch(seller_url)
        collect_from_html(html, base_url)

        # If we didn't find any ads or we suspect only a subset was
        # returned (profile pages often show only 25 items), try to
        # derive the seller's userId and fetch the full inventory list.
        # A heuristic: if fewer than 30 ads are found, attempt to use the
        # inventory list.  Many dealers have more than 30 listings.
        if len(ad_urls) < 30:
            uid = extract_user_id(html)
            if not uid:
                # As a fallback, pick the first ad link on the page and
                # fetch the listing to extract the userId from there.
                first_ad = None
                # search for any link with /s-anzeige/
                for a in BeautifulSoup(html, "html.parser").find_all("a", href=True):
                    if "/s-anzeige/" in a["href"]:
                        first_ad = urljoin(base_url, a["href"])
                        break
                if first_ad:
                    try:
                        ad_html = self._fetch(first_ad, referer=seller_url)
                        uid = extract_user_id(ad_html)
                    except Exception:
                        uid = None
            if uid:
                # Build inventory URL.  The 's-bestandsliste' endpoint
                # returns all ads for a userId in one page (server‑rendered).
                inventory_url = (
                    f"https://www.kleinanzeigen.de/s-bestandsliste.html?userId={uid}"
                )
                try:
                    inv_html = self._fetch(inventory_url, referer=seller_url)
                    # Clear previously collected ads to avoid duplicates.  Use
                    # the inventory page as the definitive source for this seller.
                    seen.clear()
                    ad_urls.clear()
                    collect_from_html(inv_html, "https://www.kleinanzeigen.de")
                except Exception:
                    # If fetching the inventory fails, keep whatever we have
                    pass

        # Remove duplicates while preserving order (should already be unique)
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

        # Extract image URLs.  We collect images from multiple sources:
        #  1) <img> tags referencing prod-ads images.  These usually
        #     point to a thumbnail; we strip query parameters to obtain
        #     the base URL.  
        #  2) JSON-LD blocks (type ImageObject) contain a ``contentUrl``
        #     property referencing a higher-resolution image.  We
        #     include these as well and normalise them by removing
        #     query parameters.  
        image_urls: List[str] = []
        # a) direct <img> tags
        for img in soup.find_all("img"):
            src = img.get("src") or ""
            if "/api/v1/prod-ads/images/" in src:
                clean = src.split("?")[0]
                if clean not in image_urls:
                    image_urls.append(clean)
            # also inspect srcset entries (comma separated)
            srcset = img.get("srcset")
            if srcset:
                for part in srcset.split(','):
                    url_part = part.strip().split(' ')[0]
                    if "/api/v1/prod-ads/images/" in url_part:
                        clean = url_part.split("?")[0]
                        if clean not in image_urls:
                            image_urls.append(clean)
        # b) JSON-LD blocks
        for script_tag in soup.find_all("script", type="application/ld+json"):
            try:
                import json
                data = json.loads(script_tag.string or "")
            except Exception:
                continue
            # data can be a dict or a list
            def extract_from(obj):
                if isinstance(obj, dict):
                    if obj.get("@type") == "ImageObject" and obj.get("contentUrl"):
                        url = obj["contentUrl"]
                        clean = url.split("?")[0]
                        if "/api/v1/prod-ads/images/" in clean and clean not in image_urls:
                            image_urls.append(clean)
                    for v in obj.values():
                        extract_from(v)
                elif isinstance(obj, list):
                    for it in obj:
                        extract_from(it)
            extract_from(data)

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
            # normalise file extension
            ext = os.path.splitext(img_url.split("?")[0])[1] or ".jpg"
            filename = f"{ad_id}_{idx+1}{ext}"
            path = os.path.join(output_dir, filename)
            try:
                # Download via the same session used for pages so that
                # cookies (e.g. consent tokens) are sent.  Provide the
                # ad URL as referer to mimic browser behaviour.
                resp = self.session.get(img_url, headers={"Referer": listing.url}, timeout=30)
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

