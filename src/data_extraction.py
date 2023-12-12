import pandas as pd
import os
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC


class Scraper:
    def __init__(self, chrome_driver_path, url):
        """
        Initialisiert den Web Scraper mit dem Pfad zum Chrome Driver und der URL der Webseite.
        """
        self.chrome_driver_path = chrome_driver_path
        self.url = url
        self.chrome_options = self._configure_chrome_options()
        self.driver = self._initialize_webdriver()

    def _configure_chrome_options(self):
        """
        Konfiguriert die Chrome-Optionen für den WebDriver.
        """
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        # Weitere Optionen können nach Bedarf hinzugefügt werden
        return chrome_options

    def _initialize_webdriver(self):
        """
        Initialisiert den Chrome WebDriver mit den konfigurierten Optionen.
        """
        driver_service = webdriver.chrome.service.Service(self.chrome_driver_path)
        driver = webdriver.Chrome(service=driver_service, options=self.chrome_options)
        driver.set_page_load_timeout(10)
        driver.set_script_timeout(10)
        return driver

    def scrape_data(self):
        """
        Startet den Web Scraper, sammelt Daten, verarbeitet sie und speichert sie in einer CSV-Datei.
        """
        self.driver.get(self.url)
        wait = WebDriverWait(self.driver, 10)
        table = wait.until(EC.visibility_of_element_located((By.CLASS_NAME, "table")))

        df = self._convert_table_to_dataframe(table)
        df_first_row = self._process_dataframe(df, 30)

        csv_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), "..", "data", "data.csv")
        self._save_to_csv(df_first_row, csv_path)

        self._display_first_row(df_first_row)
        self.driver.quit()

    def _convert_table_to_dataframe(self, table):
        """
        Konvertiert die HTML-Tabelle in einen Pandas DataFrame.
        """
        return pd.read_html(table.get_attribute("outerHTML"), thousands=".")[0]

    def _process_dataframe(self, df, row_limit):
        """
        Verarbeitet den DataFrame, ersetzt Umlaute und transformiert Komma in Punkt.
        """
        print("Original Daten vor Konvertierung:")
        print(df["Datum"])

        current_year = pd.to_datetime("today").year
        df["Datum"] = df["Datum"].apply(
            lambda x: x if isinstance(x, pd.Timestamp) else f"{x}.{current_year}" if len(str(x)) == 5 else x)

        print("Daten nach Konvertierung:")
        print(df["Datum"])

        df["Datum"] = pd.to_datetime(df["Datum"], format="%d.%m.%y", errors="coerce")
        df.columns = [self._replace_umlauts(col) for col in df.columns]
        df = self._replace_comma_with_dot(df)
        return df.head(row_limit)

    def _replace_umlauts(self, text):
        """
        Ersetzt Umlaute in einem Text.
        """
        umlaut_mapping = {'ä': 'ae', 'ö': 'oe', 'ü': 'ue', 'Ä': 'Ae', 'Ö': 'Oe', 'Ü': 'Ue', 'ß': 'ss'}
        for umlaut, replacement in umlaut_mapping.items():
            text = text.replace(umlaut, replacement)
        return text

    def _replace_comma_with_dot(self, df):
        """
        Ersetzt Komma durch Punkt in einem DataFrame.
        """
        return df.applymap(lambda x: str(x).replace(',', '.'))

    def _save_to_csv(self, df, path):
        """
        Speichert den DataFrame in einer CSV-Datei.
        """
        df.to_csv(path, index=False, decimal=',')

    def _display_first_row(self, df):
        """
        Zeigt die erste Zeile des DataFrames an.
        """
        print(df)


if __name__ == "__main__":
    # Beispielverwendung
    chrome_driver_path = r"C:\chromedriver-win64\chromedriver.exe"
    url = "https://www.boerse-frankfurt.de/aktie/siemens-energy-ag/kurshistorie/historische-kurse-und-umsaetze"

    scraper = Scraper(chrome_driver_path, url)
    scraper.scrape_data()