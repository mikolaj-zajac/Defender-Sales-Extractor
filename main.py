from __future__ import print_function

import io
import os.path
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
import zipfile
import xml.etree.ElementTree as ET
from playwright.sync_api import sync_playwright
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from dotenv import load_dotenv

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

load_dotenv()
SCOPES = ['https://www.googleapis.com/auth/spreadsheets']

LOGIN_URL = "https://defender.net.pl/panel/"
REPORT_URL = "https://defender.net.pl/panel/reports-productssold.php"
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


def perform_report_extraction():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        print("Logowanie do panelu...")
        page.goto(LOGIN_URL)
        page.wait_for_load_state("networkidle")

        page.fill("input[name='panel_login']", USERNAME)
        page.fill("input[name='panel_password']", PASSWORD)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")

        print("Przechodzenie do raportu sprzedaży...")
        page.goto(REPORT_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        page.click("label#fg_label_consider_returns1")
        page.click("label#fg_label_bundle_and_collection1")
        page.click("a.nohref[onclick*='uncheckShops']")
        page.click("label.lbl.shops[for='fg_shops0']")

        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=29)
        date_range = f"{start_date.strftime('%Y-%m-%d')} / {end_date.strftime('%Y-%m-%d')}"

        page.fill("input#fg_begin_end", "")
        page.fill("input#fg_begin_end", date_range)
        page.select_option("select#fg_sort", value="ordersQuantity")
        page.click("input.btn-primary[type='submit'][value='Pokaż']")

        print("Oczekiwanie na raport...")
        page.wait_for_load_state("networkidle")
        time.sleep(30)

        downloads_dir = Path("downloads")
        downloads_dir.mkdir(exist_ok=True)

        with page.expect_download(timeout=120000) as download_info:
            page.click("a:has-text('Eksportuj do pliku w formacie ODS')")

        download = download_info.value

        file_path = str(downloads_dir / "sold_products.ods")
        download.save_as(file_path)

        browser.close()
        return file_path


def extract_ids_from_file(file_path):
    file_ext = file_path.split('.')[-1].lower()

    if file_ext in ['csv', 'txt']:
        encodings = ['utf-8', 'cp1250', 'iso-8859-2', 'windows-1250']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, sep=';')
                break
            except:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=',')
                    break
                except:
                    continue

        if df is None:
            df = pd.read_csv(file_path, sep=None, engine='python')

    elif file_ext in ['xls', 'xlsx']:
        df = pd.read_excel(file_path)

    elif file_ext == 'ods':
        try:
            import odfpy
            df = pd.read_excel(file_path, engine='odf')
        except ImportError:
            raise Exception("odfpy nie jest zainstalowany")
    else:
        raise ValueError(f"Nieobsługiwany format: {file_ext}")

    if df.empty:
        raise ValueError("Brak danych w pliku")

    id_column = None
    for col in df.columns:
        col_str = str(col).lower()
        if any(keyword in col_str for keyword in ['iai', 'kod', 'code', 'id', 'sku', 'ean']):
            id_column = col
            break

    if id_column is None:
        id_column = df.columns[0]

    ids = []
    seen = set()

    for cell_content in df[id_column].dropna().astype(str):
        cleaned_id = cell_content.strip()

        if '\n' in cleaned_id or ',' in cleaned_id or ';' in cleaned_id:
            separators = ['\n', ',', ';']
            for sep in separators:
                if sep in cleaned_id:
                    for sub_id in cleaned_id.split(sep):
                        sub_id_clean = sub_id.strip()
                        if sub_id_clean and sub_id_clean not in seen:
                            ids.append(sub_id_clean)
                            seen.add(sub_id_clean)
                    break
        else:
            if cleaned_id and cleaned_id not in seen:
                ids.append(cleaned_id)
                seen.add(cleaned_id)

    return ids


def upload_to_google_sheets(ids):
    try:
        service = get_gsheet_service()

        values = [["id", "custom_label_2"]]
        for pid in ids:
            values.append([str(pid), "wyp"])

        body = {
            'values': values,
            'majorDimension': 'ROWS'
        }

        # Tylko update, bez clear - może brakuje uprawnień do clear
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="USER_ENTERED",  # Zmiana na USER_ENTERED
            body=body
        ).execute()

        print(f"Zaktualizowano Google Sheets. Wysłano {len(ids)} rekordów.")

    except Exception as e:
        raise Exception(f"Błąd Google Sheets: {str(e)}")


if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        init_auth_files()

    try:
        print("=" * 50)
        print(f"Rozpoczynanie procesu - {datetime.now()}")
        print("=" * 50)

        file_path = perform_report_extraction()
        print(f"Pobrano plik: {file_path}")

        ids = extract_ids_from_file(file_path)
        print(f"Znaleziono {len(ids)} ID produktów")

        upload_to_google_sheets(ids)
        print("Proces zakończony pomyślnie.")

    except Exception as e:
        print(f"BŁĄD: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)