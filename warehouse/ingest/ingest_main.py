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

    # ingest tournaments
    logging.info("Starting ingestion: tournaments")
    # ingest_tournaments()
    logging.info("Completed ingestion: tournaments")

    logging.info("Data ingestion completed.")

    # close connection
    conn.close()

if __name__ == "__main__":
    main()