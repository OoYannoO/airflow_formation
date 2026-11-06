from datetime import datetime
import os
import logging
import requests

from airflow.sdk import dag, task


@dag(
    dag_id="bitcoin_price_pipeline",
    description="ETL pipeline pour récupérer le prix du Bitcoin",
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["ETL", "CoinGecko", "Bitcoin"],
)


def bitcoin_price_pipeline():

    # ---------------------------------------
    # TASK 1 : EXTRACTION
    # ---------------------------------------

    @task
    def extract_bitcoin_price():

        url = "https://api.coingecko.com/api/v3/simple/price"

        params = {
            "ids": "bitcoin",
            "vs_currencies": "usd",
            "include_24hr_change": "true"
        }

        # Clé API facultative pour cet exemple
        api_key = os.getenv("COINGECKO_API_KEY")

        headers = {}
        if api_key:
            headers["x-cg-demo-api-key"] = api_key

        response = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=30
        )

        response.raise_for_status()

        data = response.json()

        logging.info("Données récupérées : %s", data)

        return data["bitcoin"]

    # ---------------------------------------
    # TASK 2 : TRANSFORMATION
    # ---------------------------------------

    @task
    def process_data(bitcoin_data):

        processed_data = {
            "usd": bitcoin_data["usd"],
            "change": bitcoin_data["usd_24h_change"]
        }

        logging.info(
            "Données après transformation : %s",
            processed_data
        )

        return processed_data

    # ---------------------------------------
    # TASK 3 : STOCKAGE
    # ---------------------------------------

    @task
    def store_data(processed_data):

        logging.info(
            "Enregistrement des données : %s",
            processed_data
        )

        print("================================")
        print("BITCOIN PRICE DATA")
        print("================================")
        print(f"Prix USD : {processed_data['usd']}")
        print(f"Variation 24h : {processed_data['change']} %")
        print("================================")

    # ---------------------------------------
    # DEFINITION DU WORKFLOW
    # ---------------------------------------

    bitcoin_data = extract_bitcoin_price()

    transformed_data = process_data(bitcoin_data)

    store_data(transformed_data)


# Instanciation du DAG
bitcoin_price_pipeline()
