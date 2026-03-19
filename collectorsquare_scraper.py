import time
import re
import json
import pandas as pd
from urllib.parse import urljoin

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

BASE_URL = "https://www.collectorsquare.com"
START_URL = "https://www.collectorsquare.com/en/bags.html"

HEADLESS = True
WAIT = 1.5
MAX_PAGES = None  # set to an int like 5 for testing, or None for all pages

# --------------------------------------------------
# Browser
# --------------------------------------------------
def start_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")
    options.add_argument("--window-size=1600,1200")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

# --------------------------------------------------
# Get total pages from pagination
# --------------------------------------------------
def get_total_pages(driver):
    soup = BeautifulSoup(driver.page_source, "lxml")
    pages = []

    for a in soup.select("a"):
        txt = a.get_text(strip=True)
        if txt.isdigit():
            pages.append(int(txt))

    return max(pages) if pages else 1

# --------------------------------------------------
# Extract product links from listing page
# --------------------------------------------------
def extract_product_links(driver):
    soup = BeautifulSoup(driver.page_source, "lxml")
    links = set()

    for a in soup.select("a[href]"):
        href = a["href"]
        if "/en/" in href and "/bags/" in href and href.count("/") > 4:
            links.add(urljoin(BASE_URL, href))

    return list(links)

# --------------------------------------------------
# Extract product detail data
# --------------------------------------------------
def scrape_product_page(driver, url):
    driver.get(url)
    time.sleep(WAIT)

    soup = BeautifulSoup(driver.page_source, "lxml")
    data = {"url": url}

    # Brand
    brand = soup.find("h1")
    data["brand"] = brand.get_text(strip=True) if brand else None

    # Title
    subtitle = soup.find("p")
    data["title"] = subtitle.get_text(strip=True) if subtitle else None

    # Price
    price = soup.find(text=re.compile(r"\$"))
    data["price"] = price.strip() if price else None

    # Collector ref
    text = soup.get_text(" ")
    ref = re.search(r"Collector Square Ref:\s*([\d\s]+)", text)
    data["collector_ref"] = ref.group(1) if ref else None

    # Description fields
    fields = {
        "Condition": None,
        "Collection": None,
        "Model": None,
        "Gender": None,
        "Year": None,
        "Color": None,
        "Material": None,
        "Length": None,
        "Height": None,
        "Width": None,
        "Category": None
    }

    for key in fields.keys():
        match = re.search(rf"{key}\s*:\s*([^\n]+)", text)
        if match:
            fields[key] = match.group(1).strip()

    data.update(fields)

    # Specialist comment
    comment = re.search(r"Comment from our specialist:(.*?)(Signature:|$)", text, re.S)
    data["specialist_comment"] = comment.group(1).strip() if comment else None

    return data

# --------------------------------------------------
# Main runner
# --------------------------------------------------
def main():
    driver = start_driver()
    driver.get(START_URL)
    time.sleep(WAIT)

    total_pages = get_total_pages(driver)
    if MAX_PAGES:
        total_pages = min(total_pages, MAX_PAGES)

    print(f"Total pages detected: {total_pages}")

    all_product_links = set()

    # PAGINATION LOOP
    for page in range(1, total_pages + 1):
        page_url = f"{START_URL}?page={page}"
        print(f"Scraping listing page {page}: {page_url}")

        driver.get(page_url)
        time.sleep(WAIT)

        links = extract_product_links(driver)
        all_product_links.update(links)

    print(f"Total products found: {len(all_product_links)}")

    # PRODUCT DETAIL LOOP
    results = []
    for i, link in enumerate(all_product_links, 1):
        print(f"[{i}/{len(all_product_links)}] Scraping product")
        try:
            data = scrape_product_page(driver, link)
            results.append(data)
        except Exception as e:
            print(f"Failed: {link}", e)

        time.sleep(1)

    driver.quit()

    # SAVE OUTPUT
    with open("collectorsquare_products.json", "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)

    pd.DataFrame(results).to_csv("collectorsquare_products.csv", index=False)

    print("Scraping completed successfully")

# --------------------------------------------------
if __name__ == "__main__":
    main()
