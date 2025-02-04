from ingest.utils.functions.sql import (
    create_connection,
    create_schema
)
import logging
import os

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # get schemas
    # schema_ingest = os.getenv('SCHEMA_INGESTION')
    # schema_ingest_temp = os.getenv('SCHEMA_INGESTION_TEMP')
    schema_meta = os.getenv('SCHEMA_META')

    # create connection
    conn = create_connection()

    # create cursor
    cursor = conn.cursor()

    # get list of schemas from project_parameters table
    schema_select_sql = f"""
        SELECT
            parameter_value
        FROM {schema_meta}.project_parameters
        WHERE 1=1
            AND parameter_value in ('SCHEMA_INGESTION', 'SCHEMA_INGESTION_TEMP')
            AND is_active = true
    """
    logging.info(f"Running select statement: {schema_select_sql}")
    cursor.execute(schema_select_sql)
    schema_list = [row[0] for row in cursor.fetchall()]

    # create schemas
    # for schema in [schema_ingest, schema_ingest_temp]:
    for schema in schema_list:
        create_schema(
            connection=conn,
            schema_name=schema
        )

    # close connection
    cursor.close()
    conn.close()

if __name__ == "__main__":
    main()