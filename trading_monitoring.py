import alpaca_trade_api as tradeapi
import logging
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv()
# Deine API-Schlüssel und die Basis-URL
API_KEY = os.getenv("API_KEY")
API_SECRET = os.getenv("API_SECRET")
BASE_URL = 'https://paper-api.alpaca.markets'  # Papierhandel; ändern für Live-Handel

# Logging-Einstellungen
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()


def get_account_info(api):
    """
    Ruft Kontoinformationen ab und gibt das verfügbare Kapital und die Kaufkraft aus.
    """
    try:
        account = api.get_account()
        cash = float(account.cash)
        buying_power = float(account.buying_power)

        logger.info(f"Verfügbares Kapital (Cash): {cash}")
        logger.info(f"Kaufkraft: {buying_power}")

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Kontoinformationen: {e}")

def get_current_positions(api):
    """
    Ruft die aktuellen Positionen ab und gibt diese aus.
    """
    try:
        positions = api.list_positions()

        if positions:
            logger.info("Aktuelle Positionen:")
            for position in positions:
                logger.info(f"Symbol: {position.symbol}, Menge: {position.qty}, Aktueller Preis: {position.current_price}")
        else:
            logger.info("Keine Positionen gefunden.")

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Positionen: {e}")

def get_trade_activities(api, days=30):
    """
    Ruft Handelsaktivitäten (Käufe und Verkäufe) der letzten 'days' Tage ab.
    """
    try:
        # Berechne das Startdatum und formatiere es im ISO 8601-Format (UTC)
        start_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')

        # Rufe Handelsaktivitäten ab
        activities = api.get_activities(activity_types='FILL', after=start_date)

        if activities:
            logger.info("Handelsaktivitäten (Käufe und Verkäufe):")
            for activity in activities:
                action = "Kauf" if activity.side == 'buy' else "Verkauf"
                logger.info(f"{action}: {activity.qty} Aktien von {activity.symbol} am {activity.transaction_time}, Preis: {activity.price}")
        else:
            logger.info(f"Keine Handelsaktivitäten in den letzten {days} Tagen gefunden.")

    except Exception as e:
        logger.error(f"Fehler beim Abrufen der Handelsaktivitäten: {e}")

if __name__ == "__main__":
    # Initialisiere die Alpaca API
    api = tradeapi.REST(API_KEY, API_SECRET, BASE_URL, api_version='v2')

    # Kontoinformationen anzeigen (verfügbares Kapital und Kaufkraft)
    get_account_info(api)

    # Aktuelle Positionen anzeigen
    get_current_positions(api)

    # Handelsaktivitäten der letzten 5 Tage anzeigen
    get_trade_activities(api, days=5)
