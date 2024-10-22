from utils.functions.sql import (
    create_connection,
    create_schema
)
from warehouse.ingest.ingest_tournaments import main as ingest_tournaments
import logging
import os

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # create connection
    conn = create_connection()
    
    # create ingest schemas
    logging.info(f"Creating ingestion layer schemas.")
    schema_ingest = os.getenv('SCHEMA_INGESTION')
    schema_ingest_temp = f"{schema_ingest}_temp"
    for schema in [schema_ingest, schema_ingest_temp]:
        create_schema(
            connection=conn,
            schema_name=schema
        )

    logging.info("Starting data ingestion.")

    # create configuration dictionary - used on ALL sources
    config_dict = {
        'temp_schema': schema_ingest_temp,
        'target_schema': schema_ingest,
    }

    # generate list of sources
    source_list = [
        {
            'source_name': 'tournaments',
            'ingest_func': ingest_tournaments
            'unique_column_list': ['tournament_url']
        }
    ]

    # loop through sources
    for source_dict in source_list:

        source_name = source_dict['source_name']
        ingest_func = source_dict['ingest_func']

    logging.info(f"Starting ingestion: {source_name}")
    source_config_dict = {
        **source_dict,
        **config_dict,
    }
    logging.info(f"Running ingestion for {source_name} with config: {source_config_dict}")
    ingest_func(config_dict=source_config_dict)
    logging.info(f"Completed ingestion: {source_name}")

    logging.info("Data ingestion completed.")

    # close connection
    conn.close()

if __name__ == "__main__":
    main()