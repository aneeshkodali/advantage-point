from warehouse.ingest.ingest_tournaments import main as ingest_tournaments
from utils.constants.sql import (
    SCHEMA_INGEST,
    SCHEMA_INGEST_TEMP
)
from utils.functions.sql import create_schema
import logging

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # create ingest schemas
    logging.info(f"Creating ingestion layer schemas.")
    schema_ingest_list = [SCHEMA_INGEST, SCHEMA_INGEST_TEMP]
    for schema in schema_ingest_list:
        create_schema(schema_name=schema)

    print("Starting data ingestion.")

    # ingest_tournaments()

    print("Data ingestion completed.")

if __name__ == "__main__":
    main()