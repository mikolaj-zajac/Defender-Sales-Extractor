from __future__ import print_function

import io
import os.path
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
import zipfile
import xml.etree.ElementTree as ET
import re
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

        print("1. Logowanie do panelu...")
        page.goto(LOGIN_URL)
        page.wait_for_load_state("networkidle")

        page.fill("input[name='panel_login']", USERNAME)
        page.fill("input[name='panel_password']", PASSWORD)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")

        print("2. Przechodzenie do raportu sprzedaży...")
        page.goto(REPORT_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        print("3. Ustawianie opcji raportu...")
        page.click("label#fg_label_consider_returns1")
        page.click("label#fg_label_bundle_and_collection1")
        page.click("a.nohref[onclick*='uncheckShops']")
        page.click("label.lbl.shops[for='fg_shops0']")

        print("4. Ustawianie daty...")
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=29)
        date_range = f"{start_date.strftime('%Y-%m-%d')} / {end_date.strftime('%Y-%m-%d')}"

        page.fill("input#fg_begin_end", "")
        page.fill("input#fg_begin_end", date_range)
        page.select_option("select#fg_sort", value="ordersQuantity")
        page.click("input.btn-primary[type='submit'][value='Pokaż']")

        print("5. Oczekiwanie na raport...")
        page.wait_for_load_state("networkidle")
        time.sleep(30)

        downloads_dir = Path("downloads")
        downloads_dir.mkdir(exist_ok=True)

        print("6. Pobieranie pliku ODS...")
        with page.expect_download(timeout=120000) as download_info:
            page.click("a:has-text('Eksportuj do pliku w formacie ODS')")

        download = download_info.value

        file_path = str(downloads_dir / "sold_products.ods")
        download.save_as(file_path)

        print(f"✓ Pobrano: {file_path}")
        browser.close()
        return file_path


def parse_ods_manual(ods_path):
    """Parsuje ODS bez odfpy - ręczne otwieranie zip i XML"""
    try:
        print(f"Parsuję ODS: {ods_path}")

        # 1. Otwórz jako ZIP
        with zipfile.ZipFile(ods_path, 'r') as z:
            # 2. Znajdź content.xml
            if 'content.xml' in z.namelist():
                xml_content = z.read('content.xml')
            else:
                # Szukaj innego XML
                xml_files = [f for f in z.namelist() if f.endswith('.xml')]
                if not xml_files:
                    raise ValueError("Brak plików XML w ODS")
                xml_content = z.read(xml_files[0])

        # 3. Parsuj XML
        try:
            root = ET.fromstring(xml_content)
        except ET.ParseError:
            # Jeśli problem z encodingiem
            xml_str = xml_content.decode('utf-8', errors='ignore')
            root = ET.fromstring(xml_str)

        # 4. Namespace dla ODF
        ns = {
            'office': 'urn:oasis:names:tc:opendocument:xmlns:office:1.0',
            'table': 'urn:oasis:names:tc:opendocument:xmlns:table:1.0',
            'text': 'urn:oasis:names:tc:opendocument:xmlns:text:1.0'
        }

        # 5. Zbierz wszystkie teksty
        all_texts = []
        for text_elem in root.findall('.//text:p', ns):
            if text_elem.text:
                text = text_elem.text.strip()
                if text:
                    all_texts.append(text)

        print(f"Znaleziono {len(all_texts)} fragmentów tekstu")

        # 6. Filtruj ID (kody które zawierają cyfry)
        ids = []
        for text in all_texts:
            # Jeśli tekst zawiera cyfry i ma sensowną długość
            if any(c.isdigit() for c in text) and 2 <= len(text) <= 50:
                # Sprawdź czy to wiele ID
                if '\n' in text:
                    for part in text.split('\n'):
                        part = part.strip()
                        if part and any(c.isdigit() for c in part):
                            ids.append(part)
                elif ',' in text:
                    for part in text.split(','):
                        part = part.strip()
                        if part and any(c.isdigit() for c in part):
                            ids.append(part)
                elif ';' in text:
                    for part in text.split(';'):
                        part = part.strip()
                        if part and any(c.isdigit() for c in part):
                            ids.append(part)
                else:
                    ids.append(text)

        # 7. Usuń duplikaty
        unique_ids = []
        seen = set()
        for id in ids:
            if id not in seen:
                unique_ids.append(id)
                seen.add(id)

        print(f"Wyodrębniono {len(unique_ids)} unikalnych ID")
        return unique_ids

    except Exception as e:
        print(f"Błąd parsowania ODS: {e}")
        # Fallback: prostsze parsowanie
        return parse_ods_simple(ods_path)


def parse_ods_simple(ods_path):
    """Proste parsowanie ODS - szuka kodów w tekście"""
    try:
        print("Używam prostego parsowania...")

        with zipfile.ZipFile(ods_path, 'r') as z:
            with z.open('content.xml') as f:
                content = f.read().decode('utf-8', errors='ignore')

        # Szukaj wzorców które wyglądają jak ID
        patterns = [
            r'\b[A-Za-z0-9\-_]{3,30}\b',  # Standardowe kody
            r'\b\d{4,}\b',  # Same cyfry (4+)
            r'\b[A-Z]{2,}\d{2,}\b',  # Litery + cyfry
        ]

        all_matches = []
        for pattern in patterns:
            matches = re.findall(pattern, content)
            all_matches.extend(matches)

        # Filtruj - tylko te z cyframi
        ids = []
        for match in all_matches:
            if any(c.isdigit() for c in match):
                ids.append(match)

        unique_ids = list(set(ids))
        print(f"Proste parsowanie znalazło {len(unique_ids)} ID")
        return unique_ids

    except Exception as e:
        print(f"Błąd prostego parsowania: {e}")
        return []


def extract_ids_from_file(file_path):
    file_ext = file_path.split('.')[-1].lower()
    print(f"Przetwarzam plik: {file_path} (format: {file_ext})")

    if file_ext in ['csv', 'txt']:
        print("Przetwarzanie CSV...")
        encodings = ['utf-8', 'cp1250', 'iso-8859-2', 'windows-1250']
        df = None
        for encoding in encodings:
            try:
                df = pd.read_csv(file_path, encoding=encoding, sep=';')
                print(f"✓ CSV z encoding={encoding}, separator=;")
                break
            except:
                try:
                    df = pd.read_csv(file_path, encoding=encoding, sep=',')
                    print(f"✓ CSV z encoding={encoding}, separator=,")
                    break
                except:
                    continue

        if df is None:
            df = pd.read_csv(file_path, sep=None, engine='python')
            print("✓ CSV z auto-detection")

    elif file_ext in ['xls', 'xlsx']:
        print("Przetwarzanie Excel...")
        df = pd.read_excel(file_path)
        print("✓ Wczytano Excel")

    elif file_ext == 'ods':
        # TYLKO RĘCZNE PARSOWANIE - BEZ IMPORTU odfpy!
        print("Przetwarzanie ODS (ręczne parsowanie)...")
        ids = parse_ods_manual(file_path)
        if ids:
            print(f"✓ Znaleziono {len(ids)} ID w ODS")
            return ids
        else:
            raise ValueError("Nie znaleziono ID w pliku ODS")
    else:
        raise ValueError(f"Nieobsługiwany format: {file_ext}")

    if df.empty:
        raise ValueError("Brak danych w pliku")

    print(f"Wczytano {len(df)} wierszy")
    print(f"Kolumny: {list(df.columns)}")

    id_column = None
    for col in df.columns:
        col_str = str(col).lower()
        if any(keyword in col_str for keyword in ['iai', 'kod', 'code', 'id', 'sku', 'ean']):
            id_column = col
            print(f"✓ Kolumna ID: '{col}'")
            break

    if id_column is None:
        id_column = df.columns[0]
        print(f"⚠ Używam pierwszej kolumny: '{id_column}'")

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

    print(f"Wyodrębniono {len(ids)} unikalnych ID")
    return ids


def upload_to_google_sheets(ids):
    try:
        print(f"Wysyłanie {len(ids)} ID do Google Sheets...")
        service = get_gsheet_service()

        # Przygotuj dane
        values = [["id", "custom_label_2"]]
        for pid in ids:
            values.append([str(pid), "wyp"])

        body = {'values': values}

        # Wyślij do Google Sheets
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        print(f"✓ Zaktualizowano Google Sheets")
        print(f"✓ Zakres: {result.get('updatedRange')}")
        print(f"✓ Komórki: {result.get('updatedCells')}")

    except Exception as e:
        raise Exception(f"Błąd Google Sheets: {str(e)}")


if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        init_auth_files()

    try:
        print("=" * 60)
        print(f"ROZPOCZĘCIE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # 1. Pobierz raport
        file_path = perform_report_extraction()

        # 2. Przetwórz dane
        ids = extract_ids_from_file(file_path)

        if ids:
            # 3. Wyślij do Google Sheets
            upload_to_google_sheets(ids)
            print("=" * 60)
            print("✓ SUKCES!")
            print(f"✓ Przetworzono {len(ids)} produktów")
            print("=" * 60)
        else:
            print("⚠ Brak danych do przesłania")

    except Exception as e:
        print("=" * 60)
        print("✗ BŁĄD!")
        print(f"✗ {str(e)}")
        print("=" * 60)
        import traceback

        traceback.print_exc()
        sys.exit(1)