from utils.functions.supabase.format_sql_query_results import format_sql_query_results
from typing import (
    List,
)
import logging
import os
import psycopg2

def query_control_table(
    connection: psycopg2.connect
) -> List:
    """
    Arguments:
    - connection: postgres psycopg2 connection
    
    Queries the control table for a list of records.
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # retrieve database name
    postgres_ingest_database_name = os.getenv("SUPABASE_DATABASE").upper()

    # create cursor
    cursor = connection.cursor()

    # construct query
    # prioritizes:
    # - tables that don't exist
    control_table_records_sql = f"""
        WITH
            -- select control table records
            CONTROL_TABLE as (
                SELECT
                    *
                FROM {postgres_ingest_database_name}.META.VW__CONTROL_TABLE__WEB_SCRIPTS
                WHERE IS_ACTIVE = TRUE
            )
        SELECT * FROM CONTROL_TABLE
    """
    # execute query
    # logger.debug(f"Running SQL: {control_table_records_sql}")
    cursor.execute(f"{control_table_records_sql}")

    # get results as list
    control_table_record_list = format_sql_query_results(
        cursor=cursor
    )

    # close cursor
    cursor.close()

    return control_table_record_list