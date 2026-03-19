import time
import pandas as pd
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ===============================
# CONFIG
# ===============================
HEADLESS = True
WAIT = 1.5

INPUT_FILE = r"D:\app\MKY-WebScrapper\collectorURL.xlsx"   # must contain product URLs
OUTPUT_FILE = r"D:\app\MKY-WebScrapper\product_prices.csv"

# ===============================
# START DRIVER
# ===============================
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

# ===============================
# SCRAPE PRICE
# ===============================
def scrape_price(driver, url):
    driver.get(url)
    time.sleep(WAIT)

    soup = BeautifulSoup(driver.page_source, "lxml")

    price_span = soup.select_one('[itemprop="price"]')
    currency_span = soup.select_one('[itemprop="priceCurrency"]')

    if not price_span:
        return None, None

    price = price_span.get("content") or price_span.get_text(strip=True)
    currency = currency_span.get("content") if currency_span else "USD"

    return price, currency

# ===============================
# MAIN
# ===============================
def main():
    df = pd.read_excel(INPUT_FILE)
    urls = df.iloc[:, 0].dropna().tolist()

    driver = start_driver()
    results = []

    for i, url in enumerate(urls, 1):
        print(f"[{i}/{len(urls)}] Scraping price → {url}")
        try:
            price, currency = scrape_price(driver, url)
            results.append({
                "url": url,
                "price": price,
                "currency": currency
            })
        except Exception as e:
            print("  ❌ Failed:", e)
            results.append({
                "url": url,
                "price": None,
                "currency": None
            })

    driver.quit()

    pd.DataFrame(results).to_csv(OUTPUT_FILE, index=False)
    print("\n✅ Price scraping completed")
    print("Saved to:", OUTPUT_FILE)

# ===============================
if __name__ == "__main__":
    main()
