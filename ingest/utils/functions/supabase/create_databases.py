from utils.functions.supabase.format_sql_query_results import format_sql_query_results
import logging
import os
import psycopg2

def create_databases(
    connection: psycopg2.connect
):
    """
    Arguments:
    - connection: postgres psycopg2 connection
    
    Queries the control table for a list of (unique) databases. From the list, databases are created if they do not exist.
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # retrieve database name
    postgres_ingest_database_name = os.getenv("INGESTION_DATABASE").upper()

    # create cursor
    cursor = connection.cursor()

    # construct query
    control_table_database_sql = f"""
        with
            -- select target table databases
            TARGET_DATABASE AS (
                SELECT DISTINCT
                    TARGET_DATABASE_NAME AS DATABASE_NAME
                FROM {postgres_ingest_database_name}.META.VW__CONTROL_TABLE__WEB_SCRIPTS
                WHERE IS_ACTIVE = TRUE 
            ),
            -- select temp table databases
            TEMP_DATABASE AS (
                SELECT DISTINCT
                    TEMP_DATABASE_NAME AS DATABASE_NAME
                FROM {postgres_ingest_database_name}.META.VW__CONTROL_TABLE__WEB_SCRIPTS
                WHERE IS_ACTIVE = TRUE 
            ),
            DATABASES_UNION AS (
                (SELECT DATABASE_NAME FROM TARGET_DATABASE)
                UNION
                (SELECT DATABASE_NAME FROM TEMP_DATABASE)
            ),
            DATABASES_DISTINCT AS (
                SELECT DISTINCT
                    DATABASE_NAME
                FROM DATABASES_UNION
            ),
            -- query information_schema for databases
            DATABASES_INFO_SCHEMA AS (
                SELECT
                    DATABASE_NAME
                FROM {postgres_ingest_database_name}.INFORMATION_SCHEMA.DATABASES
            ),
            -- join database datasets
            DATABASES_JOINED AS (
                SELECT
                    DATABASES_DISTINCT.DATABASE_NAME,
                    DATABASES_INFO_SCHEMA.DATABASE_NAME IS NOT NULL AS DATABASE_EXISTS_FLAG
                FROM DATABASES_DISTINCT
                LEFT JOIN DATABASES_INFO_SCHEMA ON DATABASES_DISTINCT.DATABASE_NAME = DATABASES_INFO_SCHEMA.DATABASE_NAME
            )
            SELECT * FROM DATABASES_JOINED
        ;
    """
    # execute query
    # logger.debug(f"Running SQL: {control_table_database_sql}")
    cursor.execute(f"{control_table_database_sql}")

    # get results as list
    database_list = format_sql_query_results(
        cursor=cursor
    )
    logger.debug(f"Databases returned from control table: {database_list}")

    # loop through databases
    for database_dict in database_list:

        # parse dict
        database_name = database_dict['DATABASE_NAME']
        database_exists_flag = database_dict['DATABASE_EXISTS_FLAG']

        if database_exists_flag == False:

            cursor.execute(
                f"""
                CREATE DATABASE {database_name};
                """
            )
            logger.info(f"Database created: {database_name}")

    # close cursor
    cursor.close()