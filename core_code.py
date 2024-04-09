import json
import os
import requests
import pandas as pd
from bs4 import BeautifulSoup
from datetime import datetime
import schedule
import time
import logging
import sys

def setup_logging():
    log_file = os.path.join(os.path.dirname(__file__), 'app.log')
    if not os.path.isfile(log_file):
        open(log_file, 'w').close()  # Create an empty log file

    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        filename=log_file,
        filemode='a'
    )


class TotalStation:
    def __init__(self, url, adres, region):
        self.url = url
        self.adres = adres
        self.region = region

    def get_fuel_price(self, soup, fuel_type):
        fuel_elements = soup.find_all('li')
        for fuel_element in fuel_elements:
            name_element = fuel_element.find('span', class_='name')
            if name_element and name_element.text.strip() == fuel_type:
                price_element = fuel_element.find('span', class_='prix')
                if price_element:
                    # Usunięcie dodatkowych znaków z ceny i zamiana przecinka na kropkę
                    price = price_element.text.strip().replace('€', '').replace(',', '.')
                    return float(price)
        return None

    def scrape_data(self):
        try:
            logging.info(f"Fetching data from TOTAL URL: {self.url}")
            response = requests.get(self.url)
            response.raise_for_status()
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')

            # Dodane wypisanie zawartości soup dla debugowania
            logging.debug(f"Soup contents: {soup.prettify()}")

            truck_diesel_price = self.get_fuel_price(soup, 'Truck Diesel')
            if truck_diesel_price is None:
                truck_diesel_price = self.get_fuel_price(soup, 'Diesel')

            logging.info(f"Data fetched successfully from TOTAL URL: {self.url}")
            return {
                'Region': self.region,
                'Adres': self.adres,
                'Stacja': 'TOTAL',
                'Produkt': 'Truck Diesel',
                'Cena': truck_diesel_price,
                'Data': datetime.now().strftime("%Y-%m-%d"),
                'Godzina': datetime.now().strftime("%H:%M:%S"),
                'Źródło': self.url
            }
        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from TOTAL: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unknown error processing data from TOTAL: {str(e)}")
            return None


class JetStation:
    def __init__(self, url, adres, region):
        self.url = url
        self.adres = adres
        self.region = region

    def get_fuel_price(self, soup):
        price_element = soup.find('div', class_='fuel-price product-price', id='product_price_fuel_diesel')
        if price_element:
            # Usunięcie dodatkowych znaków z ceny i zamiana przecinka na kropkę
            price = price_element.text.strip().replace('€', '').replace(',', '.')
            return float(price)
        return None

    def scrape_data(self):
        try:
            logging.info(f"Fetching data from JET URL: {self.url}")
            response = requests.get(self.url)
            response.raise_for_status()
            html_content = response.text
            soup = BeautifulSoup(html_content, 'html.parser')

            # Dodane wypisanie zawartości soup dla debugowania
            logging.debug(f"Soup contents: {soup.prettify()}")

            fuel_price = self.get_fuel_price(soup)

            logging.info(f"Data fetched successfully from JET URL: {self.url}")
            return {
                'Region': self.region,
                'Adres': self.adres,
                'Stacja': 'JET',
                'Produkt': 'Diesel',
                'Cena': fuel_price,
                'Data': datetime.now().strftime("%Y-%m-%d"),
                'Godzina': datetime.now().strftime("%H:%M:%S"),
                'Źródło': self.url
            }

        except requests.exceptions.RequestException as e:
            logging.error(f"Error fetching data from JET: {str(e)}")
            return None
        except Exception as e:
            logging.error(f"Unknown error processing data from JET: {str(e)}")
            return None


class DataProcessor:
    def __init__(self, total_url_list, jet_url_list, csv_file_path):
        self.total_url_list = total_url_list
        self.jet_url_list = jet_url_list
        self.csv_file_path = csv_file_path

    def load_data(self):
        data = []

        for item in self.total_url_list:
            url = item["link"]
            adres = item["adres"]
            region = item["region"]
            total_station = TotalStation(url, adres, region)
            total_data = total_station.scrape_data()
            if total_data:
                data.append(total_data)

        for item in self.jet_url_list:
            url = item["link"]
            adres = item["adres"]
            region = item["region"]
            jet_station = JetStation(url, adres, region)
            jet_data = jet_station.scrape_data()
            if jet_data:
                data.append(jet_data)

        if data:
            df = pd.DataFrame(data,
                              columns=['Adres', 'Region', 'Stacja', 'Produkt', 'Cena', 'Data', 'Godzina', 'Źródło'])
            return df
        else:
            return None

    def save_data(self, df):
        if not os.path.isfile(self.csv_file_path):
            # Tworzenie nowego pliku CSV i zapisanie danych
            df.to_csv(self.csv_file_path, index=False, encoding='utf-8-sig')
            logging.info(f"CSV file created and data saved: {self.csv_file_path}")
        else:
            # Odczyt istniejącego pliku CSV i dopisanie danych
            existing_df = pd.read_csv(self.csv_file_path)
            updated_df = pd.concat([existing_df, df], ignore_index=True)
            updated_df.to_csv(self.csv_file_path, index=False, encoding='utf-8-sig')
            logging.info(f"Data appended to existing CSV file: {self.csv_file_path}")

    def create_csv_file_if_not_exists(self):
        if not os.path.isfile(self.csv_file_path):

            open(self.csv_file_path, 'w').close()
            logging.info(f"Empty CSV file created: {self.csv_file_path}")

    def process_data(self):
        self.create_csv_file_if_not_exists()
        df = self.load_data()
        if df is not None:
            logging.debug("Data loaded successfully.")
            df.to_csv(self.csv_file_path, index=False, encoding="utf-8-sig")  # Ustawienie kodowania na "utf-8-sig"
            logging.debug("Data processed and saved successfully.")


def main():
    logging.info("Main function started.")

    json_file_path = os.path.join(os.path.dirname(__file__), 'stacje.json')
    csv_file_path = os.path.join(os.path.dirname(__file__), r'G:\Mój dysk\Statystyka\Dane Bazowe\ceny_paliw', 'ceny_paliw.csv')

    total_url_list, jet_url_list = read_urls_from_json(json_file_path)

    if total_url_list is None or jet_url_list is None:
        logging.error("Nie udało się wczytać wymaganych wartości z pliku konfiguracyjnego. Kończenie skryptu.")
        return

    logging.debug(f"Total URLs: {total_url_list}, Jet URLs: {jet_url_list}, CSV file path: {csv_file_path}")

    processor = DataProcessor(total_url_list, jet_url_list, csv_file_path)
    df = processor.load_data()

    if df is not None:
        logging.debug("Data loaded successfully.")
        processor.save_data(df)
        logging.debug("Data processed and saved successfully.")

    logging.info("Main function completed.")
    print("kod aktywny")  # Dodany kod
    sys.stdout.flush()  # Dodany kod


def read_urls_from_json(json_file_path):
    try:
        with open(json_file_path, 'r') as json_file:
            data = json.load(json_file)

            if not isinstance(data, list):
                logging.error("Plik konfiguracyjny JSON nie zawiera listy stacji")
                return None, None

            total_url_list = []
            jet_url_list = []

            for station in data:
                url = station.get("link")
                stacja = station.get("stacja")
                region = station.get("region")
                adres = station.get("adres")

                if url and stacja and region and adres:
                    if stacja.lower() == 'jet':
                        jet_url_list.append({'link': url, 'stacja': stacja, 'region': region, 'adres': adres})
                    elif stacja.lower() == 'total':
                        total_url_list.append({'link': url, 'stacja': stacja, 'region': region, 'adres': adres})

            os.makedirs(os.path.dirname(json_file_path), exist_ok=True)
            return total_url_list, jet_url_list

    except Exception as e:
        logging.error(f"Error reading JSON file: {str(e)}")
        return None, None


if __name__ == '__main__':
    setup_logging()

    if len(sys.argv) > 1 and sys.argv[1] == "--disable":
        print("Kod wyłączony.")
        sys.exit()

    # Ustawienie harmonogramu dla 5 różnych godzin
    schedule.every().day.at("07:00").do(main)
    schedule.every().day.at("10:00").do(main)
    schedule.every().day.at("14:00").do(main)
    schedule.every().day.at("18:00").do(main)
    schedule.every().day.at("22:00").do(main)

    while True:
        schedule.run_pending()
        time.sleep(1)

