from __future__ import print_function

import io
import os.path
import sys
import time
import pandas as pd
from datetime import datetime, timedelta
from playwright.sync_api import sync_playwright
from pathlib import Path
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.auth.transport.requests import Request
from dotenv import load_dotenv
import re

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
    """Wykonanie ekstrakcji raportu sprzedaży"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()

        # Logowanie
        print("Logowanie do panelu...")
        page.goto(LOGIN_URL)
        page.wait_for_load_state("networkidle")

        # Wypełnienie formularza logowania
        page.fill("input[name='panel_login']", USERNAME)
        page.fill("input[name='panel_password']", PASSWORD)
        page.click("button[type='submit']")
        page.wait_for_load_state("networkidle")

        # Przejście do raportu sprzedaży
        print("Przechodzenie do raportu sprzedaży...")
        page.goto(REPORT_URL)
        page.wait_for_load_state("networkidle")
        time.sleep(3)

        # Kliknięcie "tak" dla zwrotów
        print("Ustawianie opcji zwrotów...")
        try:
            page.click("label#fg_label_consider_returns1")
        except:
            page.click("label.lbl:has-text('tak') >> nth=0")

        # Kliknięcie "nie" dla bundle
        print("Ustawianie opcji bundle...")
        try:
            page.click("label#fg_label_bundle_and_collection1")
        except:
            page.click("label.lbl:has-text('nie')")

        # Odznacz wszystkie sklepy
        print("Odznaczanie wszystkich sklepów...")
        try:
            page.click("a.nohref[onclick*='uncheckShops']")
        except:
            # Alternatywny sposób na znalezienie linku
            page.click("a.nohref:has-text('odznacz wszystkie')")

        # Zaznaczenie defender.net.pl
        print("Zaznaczanie sklepu defender.net.pl...")
        try:
            page.click("label.lbl.shops[for='fg_shops0']")
        except:
            page.click("label.lbl.shops:has-text('defender.net.pl')")

        # Ustawienie daty - ostatnie 30 dni
        print("Ustawianie zakresu dat...")
        end_date = datetime.now() - timedelta(days=1)
        start_date = end_date - timedelta(days=29)

        date_range = f"{start_date.strftime('%Y-%m-%d')} / {end_date.strftime('%Y-%m-%d')}"

        # Najpierw wyczyść pole, potem wypełnij
        page.fill("input#fg_begin_end", "")
        page.fill("input#fg_begin_end", date_range)

        # Wybór sortowania po ilości sprzedanych
        print("Ustawianie sortowania...")
        page.select_option("select#fg_sort", value="ordersQuantity")

        # Kliknięcie "Pokaż"
        print("Generowanie raportu...")
        page.click("input.btn-primary[type='submit'][value='Pokaż']")

        # Oczekiwanie na załadowanie raportu
        print("Oczekiwanie na raport (30 sekund)...")
        page.wait_for_load_state("networkidle")
        time.sleep(30)

        # Pobieranie pliku w formacie CSV (łatwiejszy do przetworzenia)
        print("Przygotowanie do pobrania pliku...")
        downloads_dir = Path("downloads")
        downloads_dir.mkdir(exist_ok=True)

        # Szukamy linku do eksportu - spróbujmy pobrać CSV zamiast ODS
        try:
            # Sprawdźmy, czy jest dostępny eksport CSV
            if page.is_visible("a:has-text('Eksportuj do pliku w formacie CSV')"):
                with page.expect_download(timeout=120000) as download_info:
                    page.click("a:has-text('Eksportuj do pliku w formacie CSV')")
            else:
                # Jeśli nie ma CSV, spróbujmy ODS
                with page.expect_download(timeout=120000) as download_info:
                    page.click("a[onclick*='IAI.ods.export']:has-text('Eksportuj do pliku w formacie ODS')")
        except:
            # Alternatywny sposób - kliknij w pierwszy link eksportu
            with page.expect_download(timeout=120000) as download_info:
                page.click("a:has-text('Eksportuj do pliku')")

        download = download_info.value

        # Zapisz plik z odpowiednim rozszerzeniem
        file_extension = download.suggested_filename.split('.')[-1] if download.suggested_filename else 'csv'
        csv_path = str(downloads_dir / f"sold_products.{file_extension}")
        download.save_as(csv_path)

        print(f"Pobrano plik: {csv_path}")

        browser.close()
        return csv_path


def extract_ids_from_file(file_path):
    """Wyodrębnianie ID z pliku (CSV, Excel lub ODS)"""
    try:
        file_ext = file_path.split('.')[-1].lower()

        if file_ext in ['csv', 'txt']:
            # Próba różnych encodingów
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
                # Ostatnia próba z domyślnym encoding
                df = pd.read_csv(file_path, sep=None, engine='python')

        elif file_ext in ['xls', 'xlsx']:
            df = pd.read_excel(file_path)

        elif file_ext == 'ods':
            # Do odczytu ODS potrzebna jest biblioteka odfpy
            try:
                import odfpy
                df = pd.read_excel(file_path, engine='odf')
            except ImportError:
                raise Exception("Do odczytu plików ODS potrzebna jest biblioteka odfpy")
        else:
            raise ValueError(f"Nieobsługiwany format pliku: {file_ext}")

        if df.empty:
            raise ValueError("Brak danych w pliku")

        print(f"Znaleziono {len(df)} wierszy w pliku")
        print("Kolumn:", df.columns.tolist())

        # Szukamy kolumny z ID (Kod IAI)
        id_column = None
        for col in df.columns:
            col_str = str(col).lower()
            if any(keyword in col_str for keyword in ['iai', 'kod', 'code', 'id', 'sku', 'ean']):
                id_column = col
                break

        if id_column is None:
            # Jeśli nie znajdziemy po nazwie, próbujemy pierwszą kolumnę
            id_column = df.columns[0]
            print(f"Używam pierwszej kolumny jako ID: {id_column}")

        # Wyodrębniamy ID produktów
        ids = []
        seen = set()

        for cell_content in df[id_column].dropna().astype(str):
            # Czyszczenie ID - usuwamy białe znaki
            cleaned_id = cell_content.strip()

            # Jeśli ID zawiera wiele wartości (oddzielonych nową linią lub przecinkiem)
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
        if ids:
            print(f"Przykładowe ID: {ids[:5]}")

        return ids

    except Exception as e:
        raise Exception(f"Błąd przetwarzania pliku {file_path}: {str(e)}")


def upload_to_google_sheets(ids):
    """Wysyłanie danych do Google Sheets"""
    try:
        service = get_gsheet_service()

        values = [["id", "custom_label_2"]]
        values.extend([[pid, "wyp"] for pid in ids])

        body = {
            'values': values
        }

        # Najpierw wyczyść arkusz
        service.spreadsheets().values().clear(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
        ).execute()

        # Potem wstaw nowe dane
        result = service.spreadsheets().values().update(
            spreadsheetId=SPREADSHEET_ID,
            range=RANGE_NAME,
            valueInputOption="RAW",
            body=body
        ).execute()

        print(f"Zaktualizowano Google Sheets. Wysłano {len(ids)} rekordów.")
        print(f"Zaktualizowano komórki: {result.get('updatedCells')}")

    except Exception as e:
        raise Exception(f"Błąd Google Sheets: {str(e)}")


if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        init_auth_files()

    try:
        print("=" * 50)
        print(f"Rozpoczynanie procesu ekstrakcji raportu sprzedaży - {datetime.now()}")
        print("=" * 50)

        # Pobierz raport
        file_path = perform_report_extraction()

        # Przetwórz dane z pliku
        ids = extract_ids_from_file(file_path)

        if ids:
            # Wyślij do Google Sheets
            upload_to_google_sheets(ids)
            print(f"Proces zakończony sukcesem. Przetworzono {len(ids)} produktów.")
        else:
            print("Brak danych do przesłania.")

    except Exception as e:
        print(f"BŁĄD: {str(e)}")
        import traceback

        traceback.print_exc()
        sys.exit(1)