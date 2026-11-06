from datetime import datetime, timedelta
import logging
import pandas as pd
import pendulum

from airflow.sdk import dag, task, get_current_context
from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.sdk.definitions.deadline import (
    DeadlineAlert,
    DeadlineReference,
    AsyncCallback
)

# from plugins.deadline_callbacks import log_deadline_missed

async def log_deadline_missed(**kwargs):

    import logging

    logging.warning(
        "DEADLINE DEPASSEE : "
        "le DAG n'a pas respecté son délai."
    )

    logging.warning(
        "Informations : %s",
        kwargs
    )


# =====================================================
# CONFIGURATION
# =====================================================

POSTGRES_CONN_ID = "metadata_db"

TABLE_NAME = "dag_execution_metadata"

FILTER_DAG_ID = "dag_metadata_pipeline"


# =====================================================
# PARAMETRES DU DAG
# =====================================================

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=15)
}


# =====================================================
# DEFINITION DU DAG
# =====================================================

@dag(
    dag_id="dag_metadata_pipeline",

    description="Gestion des métadonnées des exécutions Airflow",

    start_date=pendulum.datetime(
        2026, 11, 1, 0, 0,
        tz="Europe/Paris"
    ),

    end_date=pendulum.datetime(
        2026, 12, 31, 23, 59,
        tz="Europe/Paris"
    ),

    schedule="0 12 * * *",

    catchup=True,

    default_args=default_args,

    deadline=DeadlineAlert(
        reference=DeadlineReference.DAGRUN_LOGICAL_DATE,
        interval=timedelta(hours=1),
        callback=AsyncCallback(log_deadline_missed)
    ),

    tags=["TP", "PostgreSQL", "Metadata"]

)

def dag_metadata_pipeline():

    # =================================================
    # TASK 1 : CREATION DE LA TABLE
    # =================================================

    @task
    def create_metadata_table():

        logging.info("Création de la table des métadonnées")

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        sql = """
            CREATE TABLE IF NOT EXISTS dag_execution_metadata (

                execution_date TIMESTAMP WITH TIME ZONE NOT NULL,

                dag_id VARCHAR(250) NOT NULL,

                PRIMARY KEY (execution_date, dag_id)

            );
        """

        pg_hook.run(sql)

        logging.info("Table créée ou déjà existante.")

    # =================================================
    # TASK 2 : SUPPRESSION DES DONNEES EXISTANTES
    # =================================================

    @task
    def delete_execution_metadata():

        context = get_current_context()

        execution_date = context["logical_date"]

        dag_id = context["dag"].dag_id

        logging.info(
            f"Suppression des métadonnées : "
            f"DAG={dag_id}, date={execution_date}"
        )

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        sql = """
            DELETE FROM dag_execution_metadata
            WHERE execution_date = %s
            AND dag_id = %s;
        """

        pg_hook.run(
            sql,
            parameters=(execution_date, dag_id)
        )

        logging.info("Suppression terminée.")

    # =================================================
    # TASK 3 : INSERTION DES METADONNEES
    # =================================================

    @task
    def insert_execution_metadata():

        context = get_current_context()

        execution_date = context["logical_date"]

        dag_id = context["dag"].dag_id

        logging.info(
            f"Insertion des métadonnées : "
            f"DAG={dag_id}, date={execution_date}"
        )

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        sql = """
            INSERT INTO dag_execution_metadata
            (execution_date, dag_id)
            VALUES (%s, %s);
        """

        pg_hook.run(
            sql,
            parameters=(execution_date, dag_id)
        )

        logging.info("Insertion terminée.")

    # =================================================
    # TASK 4 : EXTRACTION ET TRAITEMENT DES DONNEES
    # =================================================

    @task
    def extract_and_process_metadata():

        logging.info("Extraction des métadonnées PostgreSQL")

        pg_hook = PostgresHook(
            postgres_conn_id=POSTGRES_CONN_ID
        )

        # ---------------------------------------------
        # Lecture de toutes les données
        # ---------------------------------------------

        sql = """
            SELECT execution_date, dag_id
            FROM dag_execution_metadata
            ORDER BY execution_date;
        """

        records = pg_hook.get_records(sql)

        # ---------------------------------------------
        # Chargement dans un DataFrame Pandas
        # ---------------------------------------------

        df = pd.DataFrame(
            records,
            columns=["execution_date", "dag_id"]
        )

        logging.info("JEU DE DONNEES COMPLET :")

        logging.info("\n%s", df.to_string(index=False))

        # ---------------------------------------------
        # Filtrage sur un DAG spécifique
        # ---------------------------------------------

        filtered_df = df[
            df["dag_id"] == FILTER_DAG_ID
        ]

        logging.info(
            f"DONNEES FILTREES POUR LE DAG : {FILTER_DAG_ID}"
        )

        logging.info(
            "\n%s",
            filtered_df.to_string(index=False)
        )

        logging.info(
            f"Nombre total d'exécutions : {len(df)}"
        )

        logging.info(
            f"Nombre d'exécutions du DAG filtré : "
            f"{len(filtered_df)}"
        )

    # =================================================
    # ORCHESTRATION ET DEPENDANCES
    # =================================================

    task1 = create_metadata_table()

    task2 = delete_execution_metadata()

    task3 = insert_execution_metadata()

    task4 = extract_and_process_metadata()

    task1 >> task2 >> task3 >> task4


# Instanciation du DAG

dag_metadata_pipeline()
