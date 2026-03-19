import os
import time
import re
import requests
import pandas as pd
from urllib.parse import urlparse
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException

# ===============================
# CONFIG88
# ===============================
HEADLESS = True
WAIT = 2.5

EXCEL_PATH = r"D:\app\MKY-WebScrapper\collectorurl4-700.xlsx"
OUTPUT_DIR = r"G:\My Drive\Images"

# ===============================
# START BROWSER
# ===============================
def start_driver():
    options = webdriver.ChromeOptions()
    options.page_load_strategy = "eager"
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,1200")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    return webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )

# ===============================
# EXTRACT IMAGE URLS
# ===============================
def extract_image_urls(html, max_images=7):
    soup = BeautifulSoup(html, "lxml")
    image_urls = []

    # 🔒 STRICT SCOPE: only product image block
    container = soup.select_one("#product-photo-block")
    if not container:
        return []

    for img in container.find_all("img"):
        for attr in ["data-splide-lazy", "data-src", "src"]:
            url = img.get(attr)
            if not url:
                continue

            # ✅ only real product images
            if (
                url.startswith("http")
                and "/images/products/" in url
                and "/ots/" not in url          # ❌ ignore 360 images
                and re.search(r"\.(jpg|jpeg|png|webp)$", url, re.I)
            ):
                image_urls.append(url)
                break

    # remove duplicates, keep order
    image_urls = list(dict.fromkeys(image_urls))

    return image_urls[:max_images]


# ===============================
# DOWNLOAD IMAGE
# ===============================
def download_image(url, folder, index):
    os.makedirs(folder, exist_ok=True)

    ext = os.path.splitext(urlparse(url).path)[1]
    filename = f"image_{index}{ext}"
    filepath = os.path.join(folder, filename)

    if os.path.exists(filepath):
        return

    try:
        r = requests.get(url, timeout=30)
        r.raise_for_status()
        with open(filepath, "wb") as f:
            f.write(r.content)
    except Exception as e:
        print(f"    ❌ Failed: {url} | {e}")

# ===============================
# MAIN LOGIC
# ===============================
def download_images_for_products(product_urls):
    print("Saving images to:", OUTPUT_DIR)
    driver = start_driver()
    driver.set_page_load_timeout(120)

    for idx, product_url in enumerate(product_urls, 1):
        print(f"\n[{idx}/{len(product_urls)}] {product_url}")

        try:
            driver.get(product_url)
        except TimeoutException:
            print("  ⚠ Page load timeout, using partial page:", product_url)
            try:
                driver.execute_script("window.stop();")
            except Exception:
                pass

        time.sleep(WAIT)

        html = driver.page_source
        images = extract_image_urls(html, max_images=7)

        print(f"  ✅ Found {len(images)} images")

        product_slug = product_url.rstrip("/").split("/")[-1]
        folder = os.path.join(OUTPUT_DIR, product_slug)

        for i, img_url in enumerate(images, 1):
            download_image(img_url, folder, i)

        time.sleep(1)

    driver.quit()

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    df = pd.read_excel(EXCEL_PATH)

    # assumes URLs are in first column
    product_urls = df.iloc[:, 0].dropna().tolist()

    download_images_for_products(product_urls)
