from datetime import datetime, timedelta
import logging
import os
import time

import requests

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook


# =====================================================
# CONFIGURATION
# =====================================================

POSTGRES_CONN_ID = "bitcoin_db"

COINGECKO_USE_API = (
    os.getenv("COINGECKO_USE_API", "false").lower() == "true"
)

COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/bitcoin/history"
)


# =====================================================
# DONNEES FACTICES
# =====================================================

def generate_fake_data():

    prices = [
        65000,
        65500,
        65200,
        66000,
        65800,
        67000,
        66800,
        67500,
        68000,
        67600
    ]

    volumes = [
        25000000000,
        26000000000,
        24500000000,
        27000000000,
        25500000000,
        28000000000,
        26500000000,
        29000000000,
        30000000000,
        28500000000
    ]

    return prices, volumes


# =====================================================
# GESTION DU RATE LIMIT COINGECKO
# =====================================================

def fetch_coingecko_data(date):

    api_key = os.getenv("COINGECKO_API_KEY")

    headers = {}

    if api_key:
        headers["x-cg-demo-api-key"] = api_key

    params = {
        "date": date.strftime("%d-%m-%Y"),
        "localization": "false"
    }

    max_attempts = 5

    for attempt in range(max_attempts):

        response = requests.get(
            COINGECKO_URL,
            params=params,
            headers=headers,
            timeout=30
        )

        # Gestion du Rate Limit HTTP 429
        if response.status_code == 429:

            retry_after = response.headers.get("Retry-After")

            try:
                delay = float(retry_after)
            except (TypeError, ValueError):
                delay = 2 ** attempt

            if attempt == max_attempts - 1:
                response.raise_for_status()

            logging.warning(
                f"Rate limit atteint. Nouvelle tentative "
                f"dans {delay} secondes."
            )

            time.sleep(min(delay, 60))
            continue

        # Gestion des erreurs serveur temporaires
        if response.status_code >= 500:

            if attempt == max_attempts - 1:
                response.raise_for_status()

            delay = 2 ** attempt

            logging.warning(
                f"Erreur serveur CoinGecko. "
                f"Nouvelle tentative dans {delay} secondes."
            )

            time.sleep(delay)
            continue

        response.raise_for_status()

        data = response.json()

        market_data = data["market_data"]

        return {
            "usd": market_data["current_price"]["usd"],
            "volume": market_data["total_volume"]["usd"]
        }

    raise Exception("Impossible de récupérer les données CoinGecko")


# =====================================================
# DEFINITION DU DAG
# =====================================================

@dag(
    dag_id="bitcoin_rsi_pipeline",
    description="Pipeline ETL Bitcoin avec calcul RSI et PostgreSQL",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ETL", "Bitcoin", "RSI", "PostgreSQL"]
)

def bitcoin_rsi_pipeline():

    # =================================================
    # TASK 1 : EXTRACTION
    # =================================================

    @task
    def extract_bitcoin_price():

        context = get_current_context()

        execution_date = context["logical_date"].date()

        logging.info(
            f"Date logique d'exécution : {execution_date}"
        )

        # Calcul des 10 jours historiques
        start_date = execution_date - timedelta(days=9)

        dates = [
            start_date + timedelta(days=i)
            for i in range(10)
        ]

        # Connexion PostgreSQL
        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        # Création de la table si elle n'existe pas
        pg_hook.run("""
            CREATE TABLE IF NOT EXISTS bitcoin_market_history (
                market_date DATE PRIMARY KEY,
                price_usd NUMERIC(20, 8) NOT NULL,
                volume_usd NUMERIC(30, 8) NOT NULL
            );
        """)

        # Lecture prioritaire des données PostgreSQL
        cached_data = pg_hook.get_records(
            """
            SELECT market_date, price_usd, volume_usd
            FROM bitcoin_market_history
            WHERE market_date BETWEEN %s AND %s
            ORDER BY market_date
            """,
            parameters=(start_date, execution_date)
        )

        cache = {}

        for row in cached_data:

            cache[row[0].isoformat()] = {
                "date": row[0].isoformat(),
                "usd": float(row[1]),
                "volume": float(row[2])
            }

        logging.info(
            f"{len(cache)} journées trouvées dans PostgreSQL."
        )

        # Jeu de données fictif
        fake_prices, fake_volumes = generate_fake_data()

        historical_data = []

        # Recherche des données manquantes
        for index, current_date in enumerate(dates):

            date_key = current_date.isoformat()

            if date_key in cache:

                logging.info(
                    f"Données récupérées depuis PostgreSQL : {date_key}"
                )

                record = cache[date_key]

            else:

                logging.info(
                    f"Données absentes de PostgreSQL : {date_key}"
                )

                if COINGECKO_USE_API:

                    logging.info(
                        f"Appel API CoinGecko pour {date_key}"
                    )

                    market_data = fetch_coingecko_data(
                        current_date
                    )

                else:

                    logging.info(
                        f"Utilisation des données fictives pour {date_key}"
                    )

                    market_data = {
                        "usd": fake_prices[index],
                        "volume": fake_volumes[index]
                    }

                record = {
                    "date": date_key,
                    "usd": float(market_data["usd"]),
                    "volume": float(market_data["volume"])
                }

                # Sauvegarde dans PostgreSQL
                pg_hook.run(
                    """
                    INSERT INTO bitcoin_market_history
                    (market_date, price_usd, volume_usd)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (market_date)
                    DO UPDATE SET
                        price_usd = EXCLUDED.price_usd,
                        volume_usd = EXCLUDED.volume_usd
                    """,
                    parameters=(
                        current_date,
                        record["usd"],
                        record["volume"]
                    )
                )

            historical_data.append(record)

        logging.info(
            f"Données historiques récupérées : {historical_data}"
        )

        return historical_data

    # =================================================
    # TASK 2 : TRANSFORMATION ET CALCUL DU RSI
    # =================================================

    @task
    def process_data(historical_data):

        if len(historical_data) != 10:
            raise ValueError(
                "Le calcul du RSI nécessite 10 prix historiques."
            )

        prices = [
            item["usd"]
            for item in historical_data
        ]

        # Calcul des variations journalières
        gains = []
        losses = []

        for i in range(1, len(prices)):

            variation = prices[i] - prices[i - 1]

            if variation > 0:
                gains.append(variation)
                losses.append(0)

            else:
                gains.append(0)
                losses.append(abs(variation))

        # Moyennes des gains et pertes
        average_gain = sum(gains) / len(gains)

        average_loss = sum(losses) / len(losses)

        # Calcul du RSI
        if average_gain == 0 and average_loss == 0:

            rsi = 50.0

        elif average_loss == 0:

            rsi = 100.0

        elif average_gain == 0:

            rsi = 0.0

        else:

            rs = average_gain / average_loss

            rsi = 100 - (100 / (1 + rs))

        rsi = round(rsi, 2)

        # Récupération des données du jour d'exécution
        current_data = historical_data[-1]

        processed_data = {
            "date": current_data["date"],
            "usd": current_data["usd"],
            "volume": current_data["volume"],
            "rsi": rsi
        }

        logging.info(
            f"Résultat du calcul RSI : {processed_data}"
        )

        return processed_data

    # =================================================
    # TASK 3 : STOCKAGE / RESTITUTION
    # =================================================

    @task
    def store_data(processed_data):

        logging.info(
            "===================================="
        )

        logging.info(
            "BITCOIN HISTORICAL MARKET DATA"
        )

        logging.info(
            f"Date : {processed_data['date']}"
        )

        logging.info(
            f"Prix USD : {processed_data['usd']}"
        )

        logging.info(
            f"Volume USD : {processed_data['volume']}"
        )

        logging.info(
            f"RSI : {processed_data['rsi']}"
        )

        logging.info(
            "===================================="
        )

    # =================================================
    # ORCHESTRATION DU WORKFLOW
    # =================================================

    historical_data = extract_bitcoin_price()

    processed_data = process_data(historical_data)

    store_data(processed_data)


# Instanciation du DAG
bitcoin_rsi_pipeline()
