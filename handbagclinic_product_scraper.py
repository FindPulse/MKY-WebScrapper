"""
Scrape product details from a list of Handbag Clinic product URLs.

This script is designed for situations where you already have a list of
individual product URLs (for example, exported from the site or provided
in an Excel/CSV file).  Instead of crawling the site to discover
product links, the script will iterate over each URL, open it via
Playwright and parse out the product data.  This approach reduces
the amount of navigation and clicking required, which can make it
easier to stay under the radar of basic bot detection.

Features:

* Reads input URLs from an Excel (.xlsx) or CSV (.csv/.txt) file.
* Resumes interrupted runs by recording completed URLs in a ``done`` file.
* Connects to a running Chrome instance via the CDP (requires Chrome
  started with ``--remote-debugging-port=9222``).
* Randomizes the User‑Agent header on each run and hides the
  ``navigator.webdriver`` property to mimic a regular browser【931081005479315†L68-L81】.
* Detects when the site serves a verification or CAPTCHA page and
  pauses, allowing you to complete the challenge manually before
  continuing.
* Writes scraped rows to a CSV file with the same schema used in the
  brand‑crawler script.

Usage example::

    # First, start Chrome:
    # google-chrome --remote-debugging-port=9222 --user-data-dir=/tmp/chrome-handbagclinic

    # Then run the scraper (assuming a file 'product_urls.xlsx' with a
    # column named 'url'):
    python handbagclinic_product_scraper.py --input product_urls.xlsx --input-column url --output products.csv

    # If your file is a simple text file with one URL per line:
    python handbagclinic_product_scraper.py --input urls.txt --output products.csv

The script will prompt you when it encounters a verification page.
Simply complete the challenge in Chrome and press Enter in the terminal
to continue.

Note:  Scraping websites may violate their terms of service.  Use this
script responsibly and only for lawful purposes.
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import traceback
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

import pandas as pd  # type: ignore
from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


BASE = "https://www.handbagclinic.co.uk/"

DEFAULT_OUTPUT_CSV = "handbagclinic_product_data.csv"
DEFAULT_DONE_FILE = "handbagclinic_product_done.txt"
DEFAULT_ERROR_LOG = "handbagclinic_product_errors.log"
DEFAULT_INFO_LOG = "handbagclinic_product_info.log"

# A pool of common User‑Agent strings.  Choosing a random UA per run helps
# obscure the fact that requests originate from an automated script【931081005479315†L68-L81】.
STEALTH_USER_AGENTS = [
    # Windows Chrome
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/118.0.0.0 Safari/537.36",
    # Mac Safari
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_0) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.0 Safari/605.1.15",
    # Firefox
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:115.0) Gecko/20100101 Firefox/115.0",
    # Linux Chrome
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/117.0.0.0 Safari/537.36",
]


def log_info(msg: str, info_log: str) -> None:
    """Write an informational message to stdout and a log file."""
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(info_log, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_error(msg: str, error_log: str) -> None:
    """Write an error message to stdout and a log file."""
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(error_log, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def sleep_between(min_s: float = 1.5, max_s: float = 3.5) -> None:
    """Sleep for a random amount of time between min_s and max_s seconds."""
    time.sleep(random.uniform(min_s, max_s))


def clean_text(s: str) -> str:
    """Normalize whitespace in the given string."""
    return re.sub(r"\s+", " ", (s or "").strip())


def normalize_url(u: str) -> str:
    """Normalize a URL by stripping query parameters and fragments."""
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return u


def ensure_csv_header(output_csv: str, fieldnames: List[str]) -> None:
    """Ensure the CSV file has a header row; create it if empty."""
    if os.path.exists(output_csv) and os.path.getsize(output_csv) > 0:
        return
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def append_row(output_csv: str, row: dict, fieldnames: List[str]) -> None:
    """Append a single row to the output CSV."""
    with open(output_csv, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)


def load_done_set(done_file: str) -> Set[str]:
    """Load the set of product URLs that have already been processed."""
    if not os.path.exists(done_file):
        return set()
    with open(done_file, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def mark_done(done_file: str, url: str) -> None:
    """Append a product URL to the done file."""
    with open(done_file, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def load_urls(input_path: str, column: str) -> List[str]:
    """Load a list of URLs from an Excel or CSV/txt file."""
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")
    ext = os.path.splitext(input_path)[1].lower()
    urls: List[str] = []
    try:
        if ext in {".xlsx", ".xls"}:
            df = pd.read_excel(input_path, engine="openpyxl")
            if column not in df.columns:
                raise ValueError(
                    f"Column '{column}' not found in Excel file. Available columns: {list(df.columns)}"
                )
            urls = df[column].dropna().astype(str).tolist()
        else:
            # Assume CSV or text file with one URL per line or with a header row.
            with open(input_path, "r", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                if column in reader.fieldnames:
                    for row in reader:
                        url = row.get(column) or ""
                        if url:
                            urls.append(str(url))
                else:
                    # Treat the file as plain lines of URLs (no header)
                    f.seek(0)
                    for line in f:
                        line = line.strip()
                        if line:
                            urls.append(line)
    except Exception as e:
        raise RuntimeError(f"Failed to read URLs from {input_path}: {e}")
    # Normalize and deduplicate
    urls = [normalize_url(u) for u in urls if u]
    unique_urls: List[str] = []
    seen: Set[str] = set()
    for u in urls:
        if u not in seen:
            seen.add(u)
            unique_urls.append(u)
    return unique_urls


def extract_jsonld_blocks(soup: BeautifulSoup) -> List[dict]:
    """Extract JSON‑LD blocks from a parsed page."""
    blocks: List[dict] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        txt = tag.get_text(strip=True)
        if not txt:
            continue
        try:
            data = json.loads(txt)
        except json.JSONDecodeError:
            continue
        if isinstance(data, dict):
            blocks.append(data)
        elif isinstance(data, list):
            blocks.extend([x for x in data if isinstance(x, dict)])
    return blocks


@dataclass
class ProductRow:
    source: str
    product_url: str
    brand: str
    product_name_original: str
    product_title_internal: str
    site_code: str
    sku_internal: str
    price: str
    currency: str
    rrp: str
    saving: str
    availability: str
    overall_condition: str
    exterior_condition: str
    interior_condition: str
    hardware_condition: str
    attributes_json: str
    description_original: str
    description_internal_rewritten: str
    main_image_url: str
    gallery_image_urls: str
    scraped_at: str


def build_internal_title(brand: str, name: str, attrs: Dict[str, str]) -> str:
    """Build a synthetic product title from brand, name and selected attributes."""
    colour = attrs.get("Colour") or attrs.get("Color") or ""
    material = attrs.get("Material") or ""
    parts = [brand, name, colour, material]
    return " | ".join([p for p in parts if p])


def build_internal_sku(site_code: str, brand: str) -> str:
    """Construct a stable internal SKU from the site code and brand."""
    brand_code = re.sub(r"[^A-Z0-9]", "", brand.upper())[:6] or "BRAND"
    code = re.sub(r"[^A-Z0-9\-]", "", (site_code or "").upper()) or "NOCODE"
    return f"HBC-{brand_code}-{code}"


def rewrite_description(name: str, attrs: Dict[str, str], desc: str) -> str:
    """Generate a simple internal description with key bullet points."""
    bullets: List[str] = []
    for k in ["Colour", "Material", "Width", "Height", "Depth", "Handle Drop"]:
        if attrs.get(k):
            bullets.append(f"- {k}: {attrs[k]}")
    out: List[str] = [f"{name} (pre‑loved)."]
    if bullets:
        out.append("Key details:")
        out.extend(bullets)
    if desc:
        out.append("")
        out.append("Notes:")
        out.append(clean_text(desc))
    return "\n".join(out).strip()


def parse_product_page(html: str, url: str) -> ProductRow:
    """Parse a single product detail page and return a ProductRow instance."""
    soup = BeautifulSoup(html, "lxml")
    jsonld = extract_jsonld_blocks(soup)
    # product title
    h1 = soup.find("h1")
    product_name = clean_text(h1.get_text()) if h1 else ""
    # attempt to extract brand from breadcrumb
    brand = ""
    bc = soup.select("nav.breadcrumb a, .breadcrumb a")
    if bc and len(bc) >= 2:
        brand = clean_text(bc[1].get_text())
    if not brand:
        brand = product_name.split(" ")[0] if product_name else ""
    # price and currency
    price, currency, availability = "", "GBP", ""
    for block in jsonld:
        if block.get("@type") == "Product":
            offers = block.get("offers")
            if isinstance(offers, list) and offers:
                offers = offers[0]
            if isinstance(offers, dict):
                price = str(offers.get("price") or "")
                currency = offers.get("priceCurrency") or currency
                availability = offers.get("availability") or ""
            break
    # fallback price detection
    if not price:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"(£\s?\d[\d,]*\.\d{2})", text)
        if m:
            price = m.group(1).replace("£", "").strip()
    # site code from body text
    body_text = soup.get_text("\n", strip=True)
    site_code = ""
    mcode = re.search(r"\bCode:\s*([A-Z0-9\-]+)\b", body_text)
    if mcode:
        site_code = mcode.group(1).strip()
    # retail price and saving
    rrp, saving = "", ""
    mrrp = re.search(r"\bRRP\s*£\s*([\d,]+(?:\.\d{2})?)", body_text)
    if mrrp:
        rrp = mrrp.group(1).replace(",", "")
    msav = re.search(r"\bSAVING\s*£\s*([\d,]+(?:\.\d{2})?)", body_text)
    if msav:
        saving = msav.group(1).replace(",", "")
    # extract structured attributes from label: value lines
    attrs: Dict[str, str] = {}
    lines = [clean_text(x) for x in body_text.split("\n") if clean_text(x)]
    key_map = {
        "color": "Colour",
        "colour": "Colour",
        "handle drop": "Handle Drop",
        "overall condition": "Overall Condition",
        "exterior condition": "Exterior Condition",
        "interior condition": "Interior Condition",
        "hardware condition": "Hardware Condition",
        "width": "Width",
        "height": "Height",
        "depth": "Depth",
        "material": "Material",
        "branded dustbag": "Branded Dustbag",
    }
    for line in lines:
        if ":" in line:
            k, v = line.split(":", 1)
            kk = key_map.get(clean_text(k).lower())
            if kk:
                attrs[kk] = clean_text(v)
    # condition fields
    overall_condition = attrs.get("Overall Condition", "")
    exterior_condition = attrs.get("Exterior Condition", "")
    interior_condition = attrs.get("Interior Condition", "")
    hardware_condition = attrs.get("Hardware Condition", "")
    # description from JSON‑LD
    description_original = ""
    for block in jsonld:
        if block.get("@type") == "Product" and block.get("description"):
            description_original = clean_text(block.get("description"))
            break
    # images from JSON‑LD
    gallery_urls: List[str] = []
    for block in jsonld:
        if block.get("@type") == "Product" and block.get("image"):
            img = block["image"]
            if isinstance(img, str):
                gallery_urls.append(img)
            elif isinstance(img, list):
                gallery_urls.extend([x for x in img if isinstance(x, str)])
            break
    # fallback: collect all images in the page
    if not gallery_urls:
        for img in soup.find_all("img", src=True):
            src = img["src"].strip()
            if not src:
                continue
            abs_src = urljoin(BASE, src)
            if any(x in abs_src.lower() for x in ["logo", "icon", "sprite"]):
                continue
            gallery_urls.append(abs_src)
    # deduplicate image URLs
    dedup: List[str] = []
    seen: Set[str] = set()
    for u in gallery_urls:
        u2 = normalize_url(u)
        if u2 not in seen:
            seen.add(u2)
            dedup.append(u2)
    gallery_urls = dedup
    main_image_url = gallery_urls[0] if gallery_urls else ""
    product_title_internal = build_internal_title(brand, product_name, attrs)
    sku_internal = build_internal_sku(site_code, brand)
    description_internal = rewrite_description(product_name, attrs, description_original)
    return ProductRow(
        source="handbagclinic",
        product_url=normalize_url(url),
        brand=brand,
        product_name_original=product_name,
        product_title_internal=product_title_internal,
        site_code=site_code,
        sku_internal=sku_internal,
        price=price,
        currency=currency,
        rrp=rrp,
        saving=saving,
        availability=availability,
        overall_condition=overall_condition,
        exterior_condition=exterior_condition,
        interior_condition=interior_condition,
        hardware_condition=hardware_condition,
        attributes_json=json.dumps(attrs, ensure_ascii=False),
        description_original=description_original,
        description_internal_rewritten=description_internal,
        main_image_url=main_image_url,
        gallery_image_urls="|".join(gallery_urls),
        scraped_at=time.strftime("%Y-%m-%d %H:%M:%S"),
    )


def page_get_html(
    page,
    url: str,
    wait_selector: Optional[str] = None,
    timeout_ms: int = 45000,
    info_log: str = DEFAULT_INFO_LOG,
    error_log: str = DEFAULT_ERROR_LOG,
) -> Tuple[Optional[str], any]:
    """
    Navigate to ``url`` in the given Playwright ``page`` and return a tuple of
    ``(html, page)``.  The ``page`` object may change if the user manually
    completes a bot verification challenge in another tab.  If the page fails
    to load (due to WAF, connection resets, timeouts, etc.) this function
    will log the error and retry up to 3 times with increasing back‑off
    delays.  A small human‑like pause is inserted before returning the
    page content to mimic real browsing behaviour【931081005479315†L68-L81】.
    """
    MAX_RETRIES = 3
    RETRY_DELAYS = [0, 3, 8]
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            try:
                if wait_selector:
                    page.wait_for_selector(wait_selector, timeout=timeout_ms)
                else:
                    page.wait_for_selector(
                        "a[href*='/product/'], link[rel='next']",
                        timeout=timeout_ms,
                    )
            except Exception:
                pass
            sleep_between(1.2, 2.5)
            html = page.content()
            if html:
                lower = html.lower()
                verification_terms = [
                    "verify you are",
                    "verification",
                    "checking your browser",
                    "security check",
                    "press and hold",
                    "captcha",
                    "please wait",
                ]
                if any(term in lower for term in verification_terms):
                    log_info(
                        "Site verification detected. Please complete the verification in the browser and press Enter to continue.",
                        info_log,
                    )
                    input(
                        "[USER ACTION REQUIRED] Complete the site verification in your browser, then press Enter here to resume: "
                    )
                    try:
                        pages_list = page.context.pages
                        if pages_list:
                            page = pages_list[-1]
                    except Exception:
                        pass
                    sleep_between(1.0, 2.0)
                    html = page.content()
            return html, page
        except PWTimeoutError:
            log_error(f"Timeout loading: {url} (attempt {attempt}/{MAX_RETRIES})", error_log)
        except Exception as e:
            log_error(
                f"Browser load error: {url} err={e} (attempt {attempt}/{MAX_RETRIES})",
                error_log,
            )
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            sleep_between(delay, delay + 1)
    return None, page


def scrape_products(
    urls: List[str],
    output_csv: str,
    done_file: str,
    info_log: str,
    error_log: str,
    limit: Optional[int] = None,
) -> None:
    """Scrape product pages from a list of URLs and write to CSV."""
    done = load_done_set(done_file)
    log_info(f"Loaded done URLs: {len(done)}", info_log)
    urls_to_process = [u for u in urls if u not in done]
    if limit:
        urls_to_process = urls_to_process[:limit]
    if not urls_to_process:
        log_info("No new URLs to process.", info_log)
        return
    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        context.set_default_navigation_timeout(60000)
        try:
            ua_choice = random.choice(STEALTH_USER_AGENTS)
            context.set_extra_http_headers(
                {
                    "User-Agent": ua_choice,
                    "Accept-Language": "en-GB,en;q=0.9",
                }
            )
        except Exception:
            pass
        try:
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
        except Exception:
            pass
        page = context.new_page()
        # Block images/media/fonts to speed up scraping
        def block_resource(route, request):
            if request.resource_type in ["image", "media", "font"]:
                return route.abort()
            return route.continue_()
        try:
            page.route("**/*", block_resource)
        except Exception:
            pass
        # Determine CSV header
        fieldnames = list(
            asdict(
                ProductRow(
                    source="",
                    product_url="",
                    brand="",
                    product_name_original="",
                    product_title_internal="",
                    site_code="",
                    sku_internal="",
                    price="",
                    currency="",
                    rrp="",
                    saving="",
                    availability="",
                    overall_condition="",
                    exterior_condition="",
                    interior_condition="",
                    hardware_condition="",
                    attributes_json="",
                    description_original="",
                    description_internal_rewritten="",
                    main_image_url="",
                    gallery_image_urls="",
                    scraped_at="",
                )
            ).keys()
        )
        ensure_csv_header(output_csv, fieldnames)
        total = len(urls_to_process)
        for i, u in enumerate(urls_to_process, start=1):
            u_norm = normalize_url(u)
            log_info(f"[{i}/{total}] Scraping: {u_norm}", info_log)
            try:
                phtml, page = page_get_html(
                    page,
                    u_norm,
                    wait_selector=None,
                    info_log=info_log,
                    error_log=error_log,
                )
                if not phtml:
                    log_error(f"Product fetch failed (skipping): {u_norm}", error_log)
                    mark_done(done_file, u_norm)
                    continue
                row = asdict(parse_product_page(phtml, u_norm))
                append_row(output_csv, row, fieldnames)
                mark_done(done_file, u_norm)
            except Exception:
                log_error(
                    f"Product parse error: {u_norm}\n{traceback.format_exc()}",
                    error_log,
                )
                mark_done(done_file, u_norm)
            sleep_between()
        log_info("Completed run.", info_log)
        try:
            page.close()
        except Exception:
            pass
        try:
            browser.close()
        except Exception:
            pass


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Scrape Handbag Clinic product pages from a list of URLs."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Path to input file (.xlsx, .xls, .csv, or .txt) containing product URLs.",
    )
    parser.add_argument(
        "--input-column",
        default="url",
        help=(
            "Column name to read URLs from in the input file. If the file is a plain"
            " text file without headers, this option is ignored."
        ),
    )
    parser.add_argument(
        "--output",
        default=DEFAULT_OUTPUT_CSV,
        help=f"Path to output CSV file (default: {DEFAULT_OUTPUT_CSV}).",
    )
    parser.add_argument(
        "--done-file",
        default=DEFAULT_DONE_FILE,
        help=f"Path to file recording processed URLs (default: {DEFAULT_DONE_FILE}).",
    )
    parser.add_argument(
        "--info-log",
        default=DEFAULT_INFO_LOG,
        help=f"Path to info log file (default: {DEFAULT_INFO_LOG}).",
    )
    parser.add_argument(
        "--error-log",
        default=DEFAULT_ERROR_LOG,
        help=f"Path to error log file (default: {DEFAULT_ERROR_LOG}).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional limit on number of URLs to process (useful for testing).",
    )
    args = parser.parse_args()
    urls = load_urls(args.input, args.input_column)
    if not urls:
        print("No URLs found in input file.")
        sys.exit(1)
    scrape_products(
        urls=urls,
        output_csv=args.output,
        done_file=args.done_file,
        info_log=args.info_log,
        error_log=args.error_log,
        limit=args.limit,
    )


if __name__ == "__main__":
    main()