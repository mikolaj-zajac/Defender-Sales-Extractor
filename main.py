import pandas as pd
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from playwright.sync_api import sync_playwright
import time

LOGIN_URL = "https://defender.iai-shop.com/panel/login.php"
PRODUCTS_URL = "https://defender.iai-shop.com/panel/products-list.php?criteriaId=814951&filtersCount=2"

USERNAME = "YOUR_USERNAME"
PASSWORD = "YOUR_PASSWORD"

GOOGLE_SHEET_NAME = "Product IDs"
GOOGLE_WORKSHEET_NAME = "Sheet1"

def login_and_download_csv():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(LOGIN_URL)

        page.fill("input[name='login']", USERNAME)
        page.fill("input[name='pass']", PASSWORD)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")

        page.goto(PRODUCTS_URL)
        time.sleep(2)
        csv_button = page.locator("a[href*='export_csv']")
        if csv_button.count() == 0:
            raise Exception("CSV export button not found!")
        csv_url = csv_button.get_attribute("href")

        with page.expect_download() as download_info:
            page.click(f"a[href='{csv_url}']")
        download = download_info.value
        path = download.path()
        browser.close()
        return path

def extract_ids_from_csv(csv_path):
    df = pd.read_csv(csv_path)
    return df['ID'].dropna().astype(str).tolist()

def upload_to_google_sheets(ids):
    scope = ["https://spreadsheets.google.com/feeds", "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name("service_account.json", scope)
    client = gspread.authorize(creds)

    sheet = client.open(GOOGLE_SHEET_NAME).worksheet(GOOGLE_WORKSHEET_NAME)
    sheet.clear()
    sheet.update("A1", [["Product ID"]])
    for i, pid in enumerate(ids, start=2):
        sheet.update_cell(i, 1, pid)

if __name__ == "__main__":
    csv_path = login_and_download_csv()
    ids = extract_ids_from_csv(csv_path)
    upload_to_google_sheets(ids)
    print(f"Uploaded {len(ids)} IDs to Google Sheets.")
