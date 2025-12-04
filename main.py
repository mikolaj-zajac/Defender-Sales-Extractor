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


def scrape_products_from_page(page):
    """Scrapuje produkty bezpośrednio ze strony"""
    print("Scrapowanie danych ze strony...")

    products_data = []

    # Znajdź wszystkie wiersze tabeli z produktami
    # Szukamy wierszy z id zaczynającym się od "grid_"
    rows = page.query_selector_all('tr[id^="grid_"]')

    print(f"Znaleziono {len(rows)} wierszy z produktami")

    for i, row in enumerate(rows[:100]):  # Pierwsze 100 produktów
        try:
            # Pobierz ID z atrybutu id (np. "grid_0")
            row_id = row.get_attribute('id')

            # Szukaj komórek w wierszu
            cells = row.query_selector_all('td')

            if len(cells) >= 8:  # Powinno być 8 komórek
                # Komórka 3: ID produktu (119133-2097)
                product_id_cell = cells[3]
                product_id = product_id_cell.inner_text().strip()

                # Komórka 1: Nazwa produktu
                product_name_cell = cells[1]
                product_name = product_name_cell.inner_text().strip()

                # Komórka 2: SKU/kod
                sku_cell = cells[2]
                sku = sku_cell.inner_text().strip()

                # Komórka 5: Ilość sprzedanych
                quantity_cell = cells[5]
                quantity = quantity_cell.inner_text().strip().replace(' szt.', '')

                # Komórka 7: Łączna sprzedaż
                sales_cell = cells[7]
                sales = sales_cell.inner_text().strip().replace(' zł', '').replace(',', '.')

                # Dodaj do listy
                products_data.append({
                    'id': product_id,  # 119133-2097
                    'product_id': product_id.split('-')[0] if '-' in product_id else product_id,  # 119133
                    'variant_id': product_id.split('-')[1] if '-' in product_id and len(
                        product_id.split('-')) > 1 else '',
                    'name': product_name,
                    'sku': sku,
                    'quantity': int(quantity) if quantity.isdigit() else 0,
                    'sales': float(sales) if sales.replace('.', '', 1).isdigit() else 0.0,
                    'row_id': row_id
                })

                if (i + 1) % 20 == 0:
                    print(f"  Przetworzono {i + 1} produktów...")

        except Exception as e:
            print(f"  Błąd przetwarzania wiersza {i}: {e}")
            continue

    return products_data


def perform_direct_scraping():
    """Bezpośrednie scrapowanie ze strony bez pobierania ODS"""
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            accept_downloads=True,
            viewport={'width': 1920, 'height': 1080}
        )
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

        print("5. Generowanie raportu...")
        page.click("input.btn-primary[type='submit'][value='Pokaż']")

        print("6. Oczekiwanie na załadowanie danych...")
        page.wait_for_load_state("networkidle")
        time.sleep(10)

        # Sprawdź czy dane się załadowały
        try:
            page.wait_for_selector('tr[id^="grid_"]', timeout=30000)
        except:
            print("⚠ Nie znaleziono danych tabeli, czekam dłużej...")
            time.sleep(20)

        # Zrób screenshot dla debugowania
        page.screenshot(path='debug_page.png')
        print("✓ Zrobiono screenshot: debug_page.png")

        print("7. Scrapowanie danych z tabeli...")
        products = scrape_products_from_page(page)

        print(f"✓ Pobrano dane {len(products)} produktów")

        # Jeśli jest paginacja, możemy scrapować więcej stron
        try:
            next_button = page.query_selector('a.next_page')
            if next_button and len(products) < 100:
                print("Pobieranie następnej strony...")
                next_button.click()
                page.wait_for_load_state("networkidle")
                time.sleep(5)

                more_products = scrape_products_from_page(page)
                products.extend(more_products)

                print(f"✓ Razem pobrano {len(products)} produktów")
        except:
            pass

        browser.close()
        return products


def extract_ids_from_products(products_data):
    """Wyodrębnia ID z danych produktów"""
    ids = []

    for product in products_data:
        # Dodaj pełne ID (119133-2097)
        if product.get('id'):
            ids.append(product['id'])

        # Dodaj też samo product_id (119133) jeśli jest różne
        if product.get('product_id') and product['product_id'] != product.get('id'):
            ids.append(product['product_id'])

    # Usuń duplikaty
    unique_ids = []
    seen = set()
    for id in ids:
        if id and id not in seen:
            unique_ids.append(id)
            seen.add(id)

    print(f"Wyodrębniono {len(unique_ids)} unikalnych ID")

    # Wyświetl przykładowe ID
    if unique_ids:
        print(f"Przykładowe ID: {unique_ids[:10]}")

    return unique_ids


def upload_to_google_sheets(ids):
    try:
        print(f"Wysyłanie {len(ids)} ID do Google Sheets...")
        service = get_gsheet_service()

        # Przygotuj dane - tylko ID i custom_label_2
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


def save_products_to_csv(products_data, ids):
    """Zapisuje dane do CSV dla backupu"""
    try:
        output_dir = Path("output")
        output_dir.mkdir(exist_ok=True)

        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Zapisz pełne dane produktów
        products_csv = output_dir / f"products_full_{timestamp}.csv"
        df_products = pd.DataFrame(products_data)
        df_products.to_csv(products_csv, index=False, encoding='utf-8')
        print(f"✓ Zapisano pełne dane do: {products_csv}")

        # Zapisz tylko ID
        ids_csv = output_dir / f"products_ids_{timestamp}.csv"
        df_ids = pd.DataFrame({'id': ids, 'custom_label_2': 'wyp'})
        df_ids.to_csv(ids_csv, index=False, encoding='utf-8')
        print(f"✓ Zapisano ID do: {ids_csv}")

    except Exception as e:
        print(f"⚠ Błąd zapisu CSV: {e}")


if __name__ == "__main__":
    if os.getenv("GITHUB_ACTIONS") == "true":
        init_auth_files()

    try:
        print("=" * 60)
        print(f"ROZPOCZĘCIE - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)

        # 1. Scrapuj dane bezpośrednio ze strony
        products_data = perform_direct_scraping()

        if not products_data:
            raise ValueError("Nie udało się pobrać żadnych danych")

        # 2. Wyodrębnij ID
        ids = extract_ids_from_products(products_data)

        if ids:
            # 3. Zapisz backup do CSV
            save_products_to_csv(products_data, ids)

            # 4. Wyślij do Google Sheets
            upload_to_google_sheets(ids)

            print("=" * 60)
            print("✓ SUKCES!")
            print(f"✓ Przetworzono {len(products_data)} produktów")
            print(f"✓ Wysłano {len(ids)} ID do Google Sheets")
            print("=" * 60)
        else:
            print("⚠ Brak ID do przesłania")

    except Exception as e:
        print("=" * 60)
        print("✗ BŁĄD!")
        print(f"✗ {str(e)}")
        print("=" * 60)
        import traceback

        traceback.print_exc()
        sys.exit(1)