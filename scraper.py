import re
import json
import time
import random
import csv
import os
import traceback
from dataclasses import dataclass, asdict
from typing import Dict, List, Optional, Set, Tuple
from urllib.parse import urljoin, urlparse, urlunparse

from bs4 import BeautifulSoup
from playwright.sync_api import sync_playwright, TimeoutError as PWTimeoutError


BASE = "https://www.handbagclinic.co.uk/"
SITEMAP_BRANDS_URL = "https://www.handbagclinic.co.uk/"

OUT_CSV = "handbagclinic_products.csv"
DONE_URLS_TXT = "done_product_urls.txt"
ERROR_LOG = "scrape_errors.log"
INFO_LOG = "scrape_info.log"


def log_info(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(INFO_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")

def log_error(msg: str) -> None:
    line = f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {msg}"
    print(line)
    with open(ERROR_LOG, "a", encoding="utf-8") as f:
        f.write(line + "\n")


def sleep_between(min_s=1.8, max_s=4.5):
    time.sleep(random.uniform(min_s, max_s))


def clean_text(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").strip())

def normalize_url(u: str) -> str:
    try:
        p = urlparse(u)
        return urlunparse((p.scheme, p.netloc, p.path, "", "", ""))
    except Exception:
        return u


def load_done_set() -> Set[str]:
    if not os.path.exists(DONE_URLS_TXT):
        return set()
    with open(DONE_URLS_TXT, "r", encoding="utf-8") as f:
        return set([line.strip() for line in f if line.strip()])

def mark_done(url: str) -> None:
    with open(DONE_URLS_TXT, "a", encoding="utf-8") as f:
        f.write(url + "\n")


def ensure_csv_header(fieldnames: List[str]) -> None:
    if os.path.exists(OUT_CSV) and os.path.getsize(OUT_CSV) > 0:
        return
    with open(OUT_CSV, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writeheader()

def append_row(row: dict, fieldnames: List[str]) -> None:
    with open(OUT_CSV, "a", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=fieldnames)
        w.writerow(row)


def page_get_html(page, url: str, wait_selector: Optional[str] = None, timeout_ms: int = 45000) -> Optional[str]:
    """
    Browser fetch that survives WAF 403 for requests.
    """
    try:
        page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
        if wait_selector:
            page.wait_for_selector(wait_selector, timeout=timeout_ms)
        # small human-like pause
        sleep_between(1.2, 2.5)
        return page.content()
    except PWTimeoutError:
        log_error(f"Timeout loading: {url}")
        return None
    except Exception as e:
        log_error(f"Browser load error: {url} err={e}")
        return None


def find_brand_links_from_sitemap(html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")

    # Your screenshot: heading "Handbag Brands" then a list of links
    # We'll take all internal links in that section; selector is flexible.
    links = []
    for a in soup.find_all("a", href=True):
        txt = clean_text(a.get_text())
        href = a["href"].strip()
        if not href:
            continue
        abs_url = normalize_url(urljoin(BASE, href))
        if urlparse(abs_url).netloc != urlparse(BASE).netloc:
            continue
        if "/product/" in abs_url:
            continue
        if 2 <= len(txt) <= 40 and txt.lower() not in {"home", "sale", "buy", "sell"}:
            links.append(abs_url)

    # de-dup
    out, seen = [], set()
    for u in links:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_product_links_from_listing(html: str) -> Tuple[List[str], Optional[str]]:
    soup = BeautifulSoup(html, "lxml")
    product_urls: List[str] = []

    for a in soup.find_all("a", href=True):
        href = a["href"].strip()
        if "/product/" in href:
            product_urls.append(normalize_url(urljoin(BASE, href)))

    product_urls = list(dict.fromkeys(product_urls))

    # next page
    next_url = None
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
    blocks: List[dict] = []
    for tag in soup.find_all("script", attrs={"type": "application/ld+json"}):
        txt = tag.get_text(strip=True)
        if not txt:
            continue
        try:
            data = json.loads(txt)
            if isinstance(data, dict):
                blocks.append(data)
            elif isinstance(data, list):
                blocks.extend([x for x in data if isinstance(x, dict)])
        except json.JSONDecodeError:
            continue
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
    colour = attrs.get("Colour") or attrs.get("Color") or ""
    material = attrs.get("Material") or ""
    parts = [brand, name, colour, material]
    return " | ".join([p for p in parts if p])

def build_internal_sku(site_code: str, brand: str) -> str:
    brand_code = re.sub(r"[^A-Z0-9]", "", brand.upper())[:6] or "BRAND"
    code = re.sub(r"[^A-Z0-9\-]", "", (site_code or "").upper()) or "NOCODE"
    return f"HBC-{brand_code}-{code}"

def rewrite_description(name: str, attrs: Dict[str, str], desc: str) -> str:
    bullets = []
    for k in ["Colour", "Material", "Width", "Height", "Depth", "Handle Drop"]:
        if attrs.get(k):
            bullets.append(f"- {k}: {attrs[k]}")
    out = [f"{name} (pre-loved)."]
    if bullets:
        out.append("Key details:")
        out.extend(bullets)
    if desc:
        out.append("")
        out.append("Notes:")
        out.append(clean_text(desc))
    return "\n".join(out).strip()


def parse_product_page(html: str, url: str) -> ProductRow:
    soup = BeautifulSoup(html, "lxml")
    jsonld = extract_jsonld_blocks(soup)

    h1 = soup.find("h1")
    product_name = clean_text(h1.get_text()) if h1 else ""

    brand = ""
    bc = soup.select("nav.breadcrumb a, .breadcrumb a")
    if bc and len(bc) >= 2:
        brand = clean_text(bc[1].get_text())
    if not brand:
        brand = product_name.split(" ")[0] if product_name else ""

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

    if not price:
        text = soup.get_text(" ", strip=True)
        m = re.search(r"(£\s?\d[\d,]*\.\d{2})", text)
        if m:
            price = m.group(1).replace("£", "").strip()

    body_text = soup.get_text("\n", strip=True)
    site_code = ""
    mcode = re.search(r"\bCode:\s*([A-Z0-9\-]+)\b", body_text)
    if mcode:
        site_code = mcode.group(1).strip()

    rrp, saving = "", ""
    mrrp = re.search(r"\bRRP\s*£\s*([\d,]+(?:\.\d{2})?)", body_text)
    if mrrp:
        rrp = mrrp.group(1).replace(",", "")
    msav = re.search(r"\bSAVING\s*£\s*([\d,]+(?:\.\d{2})?)", body_text)
    if msav:
        saving = msav.group(1).replace(",", "")

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

    overall_condition = attrs.get("Overall Condition", "")
    exterior_condition = attrs.get("Exterior Condition", "")
    interior_condition = attrs.get("Interior Condition", "")
    hardware_condition = attrs.get("Hardware Condition", "")

    description_original = ""
    for block in jsonld:
        if block.get("@type") == "Product" and block.get("description"):
            description_original = clean_text(block.get("description"))
            break

    gallery_urls: List[str] = []
    for block in jsonld:
        if block.get("@type") == "Product" and block.get("image"):
            img = block["image"]
            if isinstance(img, str):
                gallery_urls.append(img)
            elif isinstance(img, list):
                gallery_urls.extend([x for x in img if isinstance(x, str)])
            break

    # fallback images
    if not gallery_urls:
        for img in soup.find_all("img", src=True):
            src = img["src"].strip()
            if not src:
                continue
            abs_src = urljoin(BASE, src)
            if any(x in abs_src.lower() for x in ["logo", "icon", "sprite"]):
                continue
            gallery_urls.append(abs_src)

    # dedup
    dedup, seen = [], set()
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


def crawl_brand_for_products(page, brand_url: str, max_pages: int = 200) -> List[str]:
    product_urls: List[str] = []
    visited: Set[str] = set()
    next_url = brand_url
    pages = 0

    while next_url and pages < max_pages:
        next_url = normalize_url(next_url)
        if next_url in visited:
            break
        visited.add(next_url)

        html = page_get_html(page, next_url)
        if not html:
            log_error(f"Listing fetch failed: {next_url}")
            break

        urls, nxt = extract_product_links_from_listing(html)
        product_urls.extend(urls)

        pages += 1
        next_url = nxt
        sleep_between()

    return list(dict.fromkeys(product_urls))


def main(limit_brands: int = 1, limit_products: int = 20):
    done = load_done_set()
    log_info(f"Loaded done URLs: {len(done)}")

    from playwright.sync_api import sync_playwright

    with sync_playwright() as p:
        # Connect to the already-open Chrome (remote debugging)
        browser = p.chromium.connect_over_cdp("http://127.0.0.1:9222")

        # Use existing context (Chrome profile/session)
        context = browser.contexts[0] if browser.contexts else browser.new_context()
        page = context.new_page()

        try:
            log_info("Using already-open Chrome tab (no goto) to avoid connection reset...")

            # Grab current tab if available; otherwise create a new one
            pages = context.pages
            if pages:
                page = pages[0]
            else:
                page = context.new_page()

           # IMPORTANT: do NOT navigate here. Just read what's already loaded.
            sleep_between(0.8, 1.5)
            html = page.content()

            if not html or len(html) < 2000:
               raise RuntimeError(
                "Could not read page content. Make sure the site is OPEN in the Chrome window "
                "you launched with --remote-debugging-port=9222."
    )


            brand_urls = find_brand_links_from_sitemap(html)
            if not brand_urls:
                raise RuntimeError("No brand URLs found on entry page.")

            if limit_brands:
                brand_urls = brand_urls[:limit_brands]
            log_info(f"Brands found: {len(brand_urls)}")

            # CSV header
            fieldnames = list(asdict(ProductRow(
                source="", product_url="", brand="", product_name_original="",
                product_title_internal="", site_code="", sku_internal="",
                price="", currency="", rrp="", saving="", availability="",
                overall_condition="", exterior_condition="", interior_condition="", hardware_condition="",
                attributes_json="", description_original="", description_internal_rewritten="",
                main_image_url="", gallery_image_urls="", scraped_at=""
            )).keys())
            ensure_csv_header(fieldnames)

            all_products: List[str] = []

            # Crawl brands
            for b in brand_urls:
                try:
                    log_info(f"Brand crawl start: {b}")
                    urls = crawl_brand_for_products(page, b)
                    log_info(f"Brand crawl done: {b} -> {len(urls)} products")
                    all_products.extend(urls)
                except Exception:
                    log_error(f"Brand crawl error: {b}\n{traceback.format_exc()}")

            all_products = list(dict.fromkeys(all_products))
            if limit_products:
                all_products = all_products[:limit_products]
            log_info(f"Total product URLs collected: {len(all_products)}")

            # Scrape products
            for i, purl in enumerate(all_products, start=1):
                purl = normalize_url(purl)
                if purl in done:
                    continue

                try:
                    log_info(f"[{i}/{len(all_products)}] Scraping: {purl}")
                    phtml = page_get_html(page, purl)
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
            # CDP: do NOT close the user's Chrome forcibly; just disconnect
            try:
                browser.close()
            except Exception:
                pass



if __name__ == "__main__":
    # Start with headless=False for first test so you can see it working
    main(limit_brands=1, limit_products=20)
