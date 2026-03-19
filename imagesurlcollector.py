import time
import re
import pandas as pd
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.common.exceptions import TimeoutException, WebDriverException

# ===============================
# CONFIG
# ===============================
HEADLESS = True
WAIT = 2.5

INPUT_EXCEL = r"D:\app\MKY-WebScrapper\collectorURL.xlsx"
OUTPUT_EXCEL = r"D:\app\MKY-WebScrapper\product_image_urls.xlsx"

MAX_IMAGES = 7
SAVE_EVERY = 10  # save every 10 products in case of failure

# ===============================
# START BROWSER
# ===============================
def start_driver():
    options = webdriver.ChromeOptions()
    if HEADLESS:
        options.add_argument("--headless=new")

    options.add_argument("--window-size=1600,1200")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(
        service=Service(ChromeDriverManager().install()),
        options=options
    )
    driver.set_page_load_timeout(300)  # increase page load timeout
    return driver

# ===============================
# EXTRACT PRODUCT IMAGE URLS ONLY
# ===============================
def extract_image_urls(html, max_images):
    soup = BeautifulSoup(html, "lxml")
    image_urls = []

    slides = soup.select(
        "#product-photo-block .splide ul.splide__list li.splide__slide img"
    )

    for img in slides:
        for attr in ["data-splide-lazy", "data-src", "src"]:
            url = img.get(attr)
            if not url:
                continue

            if (
                url.startswith("http")
                and "/images/products/" in url
                and "/ots/" not in url
                and re.search(r"\.(jpg|jpeg|png|webp)$", url, re.I)
            ):
                image_urls.append(url)
                break

    # remove duplicates, keep order
    image_urls = list(dict.fromkeys(image_urls))

    return image_urls[:max_images]

# ===============================
# MAIN LOGIC
# ===============================
def collect_image_urls(product_urls):
    driver = start_driver()
    rows = []

    for idx, product_url in enumerate(product_urls, 1):
        print(f"[{idx}/{len(product_urls)}] {product_url}")

        try:
            driver.get(product_url)
            time.sleep(WAIT)

            images = extract_image_urls(driver.page_source, MAX_IMAGES)
            print(f"  ✅ {len(images)} images found")

            row = {"product_url": product_url}
            for i, img_url in enumerate(images, 1):
                row[f"image_{i}"] = img_url

            rows.append(row)

        except (TimeoutException, WebDriverException, Exception) as e:
            print(f"  ❌ Failed to process: {product_url} | {e}")
            # still append a row with just the URL so you know it failed
            rows.append({"product_url": product_url})
            continue

        # Save progress every SAVE_EVERY URLs
        if idx % SAVE_EVERY == 0:
            pd.DataFrame(rows).to_excel(OUTPUT_EXCEL, index=False)
            print(f"\n💾 Progress saved at {idx} products\n")

    driver.quit()
    return rows

# ===============================
# RUN
# ===============================
if __name__ == "__main__":
    df = pd.read_excel(INPUT_EXCEL)
    product_urls = df.iloc[:, 0].dropna().tolist()

    data = collect_image_urls(product_urls)

    output_df = pd.DataFrame(data)
    output_df.to_excel(OUTPUT_EXCEL, index=False)

    print("\n✅ Image URLs saved to:")
    print(OUTPUT_EXCEL)
