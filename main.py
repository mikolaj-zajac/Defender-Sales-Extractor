from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import os


def setup_driver():
    """Setup Chrome driver for GitHub Actions"""
    options = Options()

    # Ustawienia dla GitHub Actions
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')

    # Opcjonalnie: wyłącz logi
    options.add_experimental_option('excludeSwitches', ['enable-logging'])

    # Użyj webdriver-manager dla automatycznej instalacji drivera
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=options)

    return driver


def extract_ids_from_file(file_path):
    """Extract IDs from ODS file with proper error handling"""
    try:
        import odf.opendocument
        from odf.table import Table, TableRow, TableCell
        from odf.text import P

        doc = odf.opendocument.load(file_path)
        tables = doc.getElementsByType(Table)

        ids = []
        for table in tables:
            rows = table.getElementsByType(TableRow)
            for row in rows:
                cells = row.getElementsByType(TableCell)
                if cells:
                    # Pobierz tekst z pierwszej komórki
                    cell_content = cells[0]
                    paragraphs = cell_content.getElementsByType(P)
                    if paragraphs:
                        text = paragraphs[0]
                        # Ekstrakcja ID (dostosuj do swoich potrzeb)
                        if text.firstChild:
                            cell_value = text.firstChild.data.strip()
                            if cell_value and cell_value.isdigit():
                                ids.append(cell_value)
        return ids

    except ImportError:
        print("ERROR: odfpy is not installed. Install it with: pip install odfpy")
        raise
    except Exception as e:
        print(f"ERROR reading ODS file: {str(e)}")
        return []


# Przykład użycia w głównej funkcji
def main():
    print("=" * 50)
    print(f"Starting process - {pd.Timestamp.now()}")
    print("=" * 50)

    # Setup driver for GitHub Actions
    driver = setup_driver()

    try:
        # Twój kod logowania i pobierania pliku...
        print("Logging in...")
        # ... reszta kodu ...

        if os.path.exists("downloads/sold_products.ods"):
            ids = extract_ids_from_file("downloads/sold_products.ods")
            print(f"Extracted {len(ids)} IDs")

            # Zapisz do Excel
            df = pd.DataFrame({"ID": ids})
            df.to_excel("extracted_ids.xlsx", index=False)
            print("Saved to extracted_ids.xlsx")
        else:
            print("ERROR: ODS file not found")

    finally:
        driver.quit()


if __name__ == "__main__":
    main()