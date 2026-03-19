"""
Enhanced handbagclinic.co.uk scraper.

This module contains a Playwright based crawler for the Handbag Clinic website. It
parses brand listing pages and individual product detail pages to build a CSV
dataset.  The crawler connects to an already running Chrome instance via
remote debugging and reuses the existing session cookies to bypass web
application firewalls (WAF) that block bots.  Compared to the original
implementation this version adds a few quality of life improvements:

* Robust retry logic for all page navigations with exponential back‑off.  This
  helps survive intermittent connection resets or QUIC protocol errors by
  automatically retrying failed page loads rather than failing outright.
* Custom HTTP headers (User‑Agent and Accept‑Language) on the Playwright
  context to better mimic a real browser session, which can help avoid bot
  detection.
* Optional request interception to disable loading of images, fonts and
  media assets.  Turning off unnecessary resources reduces bandwidth and
  speeds up the crawl considerably, while still loading HTML and scripts.
* Automatic fallback to navigating to the home page when the initial tab
  content is empty.  If the user forgets to open the site manually before
  running the script, the scraper will attempt to open the base URL on its
  own.

To run the scraper launch Chrome with the remote debugging port enabled,
navigate to https://www.handbagclinic.co.uk/ in at least one tab, and then
execute this script.  The script will crawl one brand and up to 20 products
by default, writing results to ``handbagclinic_products.csv``.  The done
product URLs and logs are persisted across runs so the scraper can be
interrupted and resumed safely.

"""

import csv
import json
import os
import random
import re
import time
import traceback
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


BASE = "https://www.handbagclinic.co.uk/"
# On handbagclinic.co.uk the list of all brands lives under the "handbags"
# category.  The home page contains only a handful of featured links, so
# pointing the sitemap URL to the handbags landing page yields a more
# comprehensive list of brands to crawl.  This can still be overridden at
# runtime but provides better defaults.
SITEMAP_BRANDS_URL = "https://www.handbagclinic.co.uk/handbags"

OUT_CSV = "handbagclinic_products.csv"
DONE_URLS_TXT = "done_product_urls.txt"
ERROR_LOG = "scrape_errors.log"
INFO_LOG = "scrape_info.log"

# Configuration for navigation retries.  If a page fails to load due to a
# transient network error, the scraper will wait for the given delay and
# retry.  You can adjust the number of retries and delays here.
MAX_RETRIES = 3
RETRY_DELAYS = [0, 3, 8]  # seconds to sleep before retrying on 2nd/3rd attempts

# A pool of common User‑Agent strings.  Choosing a random UA per run helps
# obscure the fact that requests originate from an automated script.  See
# https://developers.whatismybrowser.com/useragents/explore/ for examples.
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


def log_info(msg: str) -> None:
    """Write an informational message to stdout and the info log."""
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(INFO_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def log_error(msg: str) -> None:
    """Write an error message to stdout and the error log."""
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def sleep_between(min_s: float = 1.8, max_s: float = 4.5) -> None:
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


def load_done_set() -> Set[str]:
    """Load the set of product URLs that have already been processed."""
    if not os.path.exists(DONE_URLS_TXT):
        return set()
    with open(DONE_URLS_TXT, "r", encoding="utf-8") as f:
        return {line.strip() for line in f if line.strip()}


def mark_done(url: str) -> None:
    """Append a product URL to the done file."""
    with open(DONE_URLS_TXT, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def ensure_csv_header(fieldnames: List[str]) -> None:
    """Ensure the CSV file has a header row; create it if empty."""
    if os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0:
        return
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()


def append_row(row: dict, fieldnames: List[str]) -> None:
    """Append a single row to the output CSV."""
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)


def page_get_html(
    page,
    url: str,
    wait_selector: Optional[str] = None,
    timeout_ms: int = 45000,
) -> Tuple[Optional[str], any]:
    """
    Navigate to ``url`` in the given Playwright ``page`` and return a tuple of
    ``(html, page)``.  The ``page`` object may change if the user manually
    completes a bot verification challenge in another tab.  If the page fails
    to load (due to WAF, connection resets, timeouts, etc.) this function
    will log the error and retry up to ``MAX_RETRIES`` times with increasing
    back‑off delays.  A small human‑like pause is inserted before returning
    the page content to mimic real browsing behaviour.
    """
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            # Navigate to the URL and wait for DOMContentLoaded.  Many pages on
            # handbagclinic.co.uk load products via client‑side JavaScript after
            # the initial HTML is parsed.  To ensure the product anchors appear
            # in the DOM, we optionally wait for a CSS selector.  If no selector
            # is provided, we wait for either a product link or a next‑page link
            # to appear.  Using a selector helps Playwright wait for the
            # asynchronous content rather than returning too early.
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
                # It's okay if the selector doesn't show up; we still proceed.
                pass
            # Small delay to mimic human behaviour and allow lazy scripts to finish
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
                        "Site verification detected. Please complete the verification in the browser and press Enter to continue."
                    )
                    input(
                        "[USER ACTION REQUIRED] Complete the site verification in your browser, then press Enter here to resume: "
                    )
                    # After manual verification the user may have opened a new tab
                    # or navigated away.  Grab the latest page from the context
                    # to ensure we are attached to the current tab.
                    try:
                        pages_list = page.context.pages
                        if pages_list:
                            page = pages_list[-1]
                    except Exception:
                        pass
                    # Give a brief pause after manual verification to let the page load
                    sleep_between(1.0, 2.0)
                    html = page.content()
            return html, page
        except PWTimeoutError:
            log_error(f"Timeout loading: {url} (attempt {attempt}/{MAX_RETRIES})")
        except Exception as e:
            log_error(f"Browser load error: {url} err={e} (attempt {attempt}/{MAX_RETRIES})")
        if attempt < MAX_RETRIES:
            delay = RETRY_DELAYS[min(attempt, len(RETRY_DELAYS) - 1)]
            sleep_between(delay, delay + 1)
    # Failed all attempts
    return None, page


def find_brand_links_from_sitemap(html: str) -> List[str]:
    """Extract brand listing links from the sitemap/home page HTML."""
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.find_all("a", href=True):
        txt = clean_text(a.get_text())
        href = a["href"].strip()
        if not href:
            continue
        abs_url = normalize_url(urljoin(BASE, href))
        # only keep internal links
        if urlparse(abs_url).netloc != urlparse(BASE).netloc:
            continue
        # skip obvious product or other non‑category pages
        if "/product/" in abs_url:
            continue
        if 2 <= len(txt) <= 40 and txt.lower() not in {"home", "sale", "buy", "sell"}:
            links.append(abs_url)
    # deduplicate while preserving order
    out: List[str] = []
    seen: Set[str] = set()
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_product_links_from_listing(html: str) -> Tuple[List[str], Optional[str]]:
    """Parse a brand listing page and return product URLs and the next page URL."""
    soup = BeautifulSoup(html, "lxml")
    product_urls: List[str] = []
    # gather product links
    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/product/" in href:
            product_urls.append(normalize_url(urljoin(BASE, href)))
    product_urls = list(dict.fromkeys(product_urls))
    # find next page link
    next_url: Optional[str] = None
    # Some pages include a <link rel="next"> tag in the head instead of
    # rendering an anchor.  Prefer that if present because it is always
    # available even when the product list uses infinite scroll.  Fall back
    # to <a rel="next"> or text‑based "Next" links otherwise.
    link_next = soup.find("link", attrs={"rel": "next"})
    if link_next and link_next.get("href"):
        next_url = normalize_url(urljoin(BASE, link_next["href"]))
    if not next_url:
        a_next = soup.find("a", attrs={"rel": "next"})
        if a_next and a_next.get("href"):
            next_url = normalize_url(urljoin(BASE, a_next["href"]))
    if not next_url:
        for a in soup.find_all("a", href=True):
            t = clean_text(a.get_text()).lower()
            if t in {"next", "next page", ">", "→"}:
                next_url = normalize_url(urljoin(BASE, a["href"]))
                break
    return product_urls, next_url


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


def crawl_brand_for_products(
    page, brand_url: str, max_pages: int = 200
) -> Tuple[List[str], any]:
    """
    Iterate over a brand's listing pages and return a tuple of
    (product_urls, page).  The ``page`` object may change during the crawl
    if the user manually solves a verification challenge that opens a new tab.
    """
    product_urls: List[str] = []
    visited: Set[str] = set()
    next_url: Optional[str] = brand_url
    pages_count = 0
    while next_url and pages_count < max_pages:
        next_url = normalize_url(next_url)
        if next_url in visited:
            break
        visited.add(next_url)
        # Wait for product links to appear on the listing page.  Without
        # specifying a selector Playwright may return before the client‑side
        # catalogue is hydrated, yielding zero products.  We look for
        # anchors pointing at product pages.
        html, page = page_get_html(page, next_url, wait_selector="a[href*='/product/']")
        if not html:
            log_error(f"Listing fetch failed: {next_url}")
            break
        urls, nxt = extract_product_links_from_listing(html)
        product_urls.extend(urls)
        pages_count += 1
        next_url = nxt
        sleep_between()
    return list(dict.fromkeys(product_urls)), page


def main(limit_brands: int = 1, limit_products: int = 20) -> None:
    """Entry point for running the scraper from the command line."""
    done = load_done_set()
    log_info(f"Loaded done URLs: {len(done)}")
    with sync_playwright() as p:
        # Connect to the already open Chrome via remote debugging.  See
        # https://playwright.dev/python/docs/./browsers#attach-to-existing-browser-instance
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")
        # Use an existing context (tab) if available; otherwise create a new one
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        # Set default navigation timeout to something generous
        context.set_default_navigation_timeout(60000)
        # Choose a random User-Agent to reduce detection.  Pick from a pool of
        # popular browser signatures; if the context cannot accept extra
        # headers we silently ignore the error.  Also set Accept-Language to
        # match a UK/English locale.  See
        # https://developers.whatismybrowser.com/useragents/explore/ for more.
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
        # Patch the webdriver property to undefined to make it harder for
        # detection scripts to identify Playwright.  This executes in every
        # page context before any other scripts.  If unsupported (e.g. CDP
        # contexts may reject add_init_script) we silently ignore.
        try:
            context.add_init_script(
                "Object.defineProperty(navigator, 'webdriver', { get: () => undefined });"
            )
        except Exception:
            pass
        page = context.new_page()
        # Optionally block images and media to speed up crawling
        def block_resource(route, request):
            if request.resource_type in ["image", "media", "font"]:
                return route.abort()
            return route.continue_()
        try:
            page.route("**/*", block_resource)
        except Exception:
            pass
        try:
            log_info(
                "Using already‑open Chrome tab (will attempt to reuse session cookies to avoid WAF)."
            )
            # If there are pages open, pick the first one; else open a new tab
            pages = context.pages
            if pages:
                page = pages[0]
            else:
                page = context.new_page()
            # Read current tab content; if it is empty, try to navigate to the base URL
            sleep_between(0.8, 1.5)
            html = page.content()
            if not html or len(html) < 2000:
                log_info(
                    "Initial tab content appears empty. Attempting to navigate to base URL to initialise session."
                )
                # Do not use retries here; rely on page_get_html for robustness
                test_html, page = page_get_html(page, BASE)
                if test_html:
                    html = test_html
                else:
                    raise RuntimeError(
                        "Could not read page content. Make sure the site is open in the Chrome window "
                        "you launched with --remote-debugging-port=9222 or reachable via the network."
                    )
            brand_urls = find_brand_links_from_sitemap(html)
            # If the initial tab didn't yield any brand links or only a few generic links
            # (for example if the user opened the home page rather than the handbags
            # category), fall back to loading the dedicated handbags landing page to
            # discover the brand catalogue.  This second fetch uses page_get_html
            # which will wait for the page to fully render and return dynamic
            # content.
            if not brand_urls or len(brand_urls) < 3:
                log_info("Retrying brand discovery from handbags landing page...")
                alt_html, page = page_get_html(page, SITEMAP_BRANDS_URL)
                if alt_html:
                    brand_urls = find_brand_links_from_sitemap(alt_html)
            if not brand_urls:
                raise RuntimeError("No brand URLs found on entry page or handbags page.")
            if limit_brands:
                brand_urls = brand_urls[:limit_brands]
            log_info(f"Brands found: {len(brand_urls)}")
            # Determine CSV header from dataclass fields
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
            ensure_csv_header(fieldnames)
            all_products: List[str] = []
            # Crawl each brand
            for b in brand_urls:
                try:
                    log_info(f"Brand crawl start: {b}")
                    urls, page = crawl_brand_for_products(page, b)
                    log_info(f"Brand crawl done: {b} -> {len(urls)} products")
                    all_products.extend(urls)
                except Exception:
                    log_error(f"Brand crawl error: {b}\n{traceback.format_exc()}")
            # deduplicate and apply limit
            all_products = list(dict.fromkeys(all_products))
            if limit_products:
                all_products = all_products[:limit_products]
            log_info(f"Total product URLs collected: {len(all_products)}")
            # Scrape each product
            for i, purl in enumerate(all_products, start=1):
                purl = normalize_url(purl)
                if purl in done:
                    continue
                try:
                    log_info(f"[{i}/{len(all_products)}] Scraping: {purl}")
                    phtml, page = page_get_html(page, purl)
                    if not phtml:
                        log_error(f"Product fetch failed (skipping): {purl}")
                        mark_done(purl)
                        done.add(purl)
                        continue
                    row = asdict(parse_product_page(phtml, purl))
                    append_row(row, fieldnames)
                    mark_done(purl)
                    done.add(purl)
                except Exception:
                    log_error(f"Product parse error: {purl}\n{traceback.format_exc()}")
                    mark_done(purl)
                    done.add(purl)
                sleep_between()
            log_info("Completed run.")
        finally:
            try:
                page.close()
            except Exception:
                pass
            # Do not close the user's Chrome; just disconnect
            try:
                browser.close()
            except Exception:
                pass


if __name__ == "__main__":
    # When running directly, crawl one brand and up to 20 products by default
    main(limit_brands=1, limit_products=20)