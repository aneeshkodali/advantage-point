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
    postgres_ingest_database_name = os.getenv("INGESTION_DATABASE")

    # create cursor
    cursor = connection.cursor()

    # construct query
    # prioritizes:
    # - tables that don't exist
    control_table_records_sql = f"""
        with
            -- select control table records
            control_table as (
                select
                    *
                from {postgres_ingest_database_name}.meta.vw__control_table__web_scripts
                where is_active = true
            )
        select * from control_table
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