from __future__ import print_function
import os.path
import time
import pandas as pd
from playwright.sync_api import sync_playwright
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from dotenv import load_dotenv

load_dotenv()
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

LOGIN_URL = "https://defender.iai-shop.com/panel/products-search.php?form=extended"
SEARCH_URL = "https://defender.iai-shop.com/panel/products-search.php?form=extended"
USERNAME = os.getenv("IAI_USERNAME")
PASSWORD = os.getenv("IAI_PASSWORD")

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID")
RANGE_NAME = "Arkusz1!A1"

def init_auth_files():
    Path("credentials.json").write_text(os.environ["GOOGLE_CREDENTIALS"])
    Path("token.json").write_text(os.environ["GOOGLE_TOKEN"])

def get_gsheet_service():
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())
    return build('sheets', 'v4', credentials=creds)


def perform_search_and_export():
    """Wykonanie wyszukiwania i eksportu"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        page.goto(LOGIN_URL)
        page.fill("input[name='panel_login']", USERNAME)
        page.fill("input[name='panel_password']", PASSWORD)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")
        page.goto(SEARCH_URL)

        time.sleep(2)

        page.click("label#fg_label_wsp1")
        page.click("label#fg_label_przecb1")
        page.click("input#deliveryButton")

        time.sleep(30)

        page.click("span.lbl")
        page.click("a.nohref[onclick*='checkAllPage']")

        time.sleep(10)

        page.click("input#productsExportAction")
        page.click("a#choice_export_toplayer2")
        page.click("a.nohref:has-text('moto-tour.com.pl')")

        downloads_dir = Path("downloads")
        downloads_dir.mkdir(exist_ok=True)

        with page.expect_download(timeout=600000) as download_info:
            pass

        download = download_info.value

        csv_path = str(downloads_dir / "exported_products.csv")
        download.save_as(csv_path)

        browser.close()
        return csv_path


def extract_ids_from_csv(csv_path):
    """Wyciąganie ID z CSV"""
    try:
        df = pd.read_csv(csv_path, encoding='utf-8')
        if '@id' not in df.columns:
            raise ValueError("Brak kolumny '@id' w pliku CSV")
        return df['@id'].dropna().astype(str).tolist()
    except Exception as e:
        raise Exception(f"Błąd przetwarzania CSV: {str(e)}")


def upload_to_google_sheets(ids):
    """Wysyłanie danych do Google Sheets"""
    try:
        service = get_gsheet_service()
        body = {
            'values': [["id"]] + [[pid] for pid in ids]
        }
        service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()
    except Exception as e:
        raise Exception(f"Błąd Google Sheets: {str(e)}")


if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        init_auth_files()
    try:
        print("Rozpoczynanie procesu...")
        csv_path = perform_search_and_export()
        print(f"Pobrano plik: {csv_path}")

        ids = extract_ids_from_csv(csv_path)
        print(f"Znaleziono {len(ids)} ID produktów")

        upload_to_google_sheets(ids)
        print(f"Zaktualizowano Google Sheets. Wysłano {len(ids)} rekordów.")

    except Exception as e:
        print(f"BŁĄD: {str(e)}")
        raise
