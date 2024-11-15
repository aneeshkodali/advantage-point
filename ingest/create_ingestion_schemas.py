from utils.functions.sql import (
    create_connection,
    create_schema
)
import logging
import os

# set logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

# get schemas
schema_ingest = os.getenv('SCHEMA_INGESTION')
schema_ingest_temp = os.getenv('SCHEMA_INGESTION_TEMP')

# create connection
conn = create_connection()

# create schemas
for schema in [schema_ingest, schema_ingest_temp]:
    create_schema(
        connection=conn,
        schema_name=schema
    )

# close connection
conn.close()