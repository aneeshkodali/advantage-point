from utils.functions.supabase.format_sql_query_results import format_sql_query_results
import logging
import os
import psycopg2

def create_schemas(
    connection: psycopg2.connect
):
    """
    Arguments:
    - connection: postgres psycopg2 connection
    
    Queries the control table for a list of (unique) schemas. From the list, schemas are created if they do not exist.
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # retrieve database name
    postgres_ingest_database_name = os.getenv("SUPABASE_DATABASE").upper()

    # create cursor
    cursor = connection.cursor()

    # construct query
    control_table_schema_sql = f"""
        WITH
            -- select target table schemas
            TARGET_SCHEMA AS (
                SELECT DISTINCT
                    TARGET_SCHEMA_NAME AS SCHEMA_NAME
                FROM {postgres_ingest_database_name}.META.VW__CONTROL_TABLE__WEB_SCRIPTS
                WHERE IS_ACTIVE = TRUE 
            ),
            -- select temp table schemas
            TEMP_SCHEMA AS (
                SELECT DISTINCT
                    TEMP_SCHEMA_NAME AS SCHEMA_NAME
                FROM {postgres_ingest_database_name}.META.VW__CONTROL_TABLE__WEB_SCRIPTS
                WHERE IS_ACTIVE = TRUE 
            ),
            SCHEMAS_UNION AS (
                (SELECT SCHEMA_NAME FROM TARGET_SCHEMA)
                UNION
                (SELECT SCHEMA_NAME FROM TEMP_SCHEMA)
            ),
            SCHEMAS_DISTINCT AS (
                SELECT DISTINCT
                    SCHEMA_NAME
                FROM SCHEMAS_UNION
            ),
            -- query information_schema for schemas
            SCHEMAS_INFO_SCHEMA AS (
                SELECT
                    SCHEMA_NAME
                FROM {postgres_ingest_database_name}.INFORMATION_SCHEMA.SCHEMATA
            ),
            -- join schema datasets
            SCHEMAS_JOINED AS (
                SELECT
                    SCHEMAS_DISTINCT.SCHEMA_NAME,
                    SCHEMAS_INFO_SCHEMA.SCHEMA_NAME IS NOT NULL AS SCHEMA_EXISTS_FLAG
                FROM SCHEMAS_DISTINCT
                LEFT JOIN SCHEMAS_INFO_SCHEMA ON 1=1
                    AND SCHEMAS_DISTINCT.SCHEMA_NAME = SCHEMAS_INFO_SCHEMA.SCHEMA_NAME
            )
            SELECT * FROM SCHEMAS_JOINED
        ;
    """
    # execute query
    # logger.info("Checking/creating required schemas from control table records")
    # logger.debug(f"Running SQL: {control_table_schema_sql}")
    cursor.execute(f"{control_table_schema_sql}")

    # get results as list
    schema_list = format_sql_query_results(
        cursor=cursor
    )

    if not schema_list:
        logger.warning("No schema records found from control table query.")


    # loop through schemas
    for schema_dict in schema_list:

        # parse dict
        schema_name = schema_dict['schema_name']
        schema_exists_flag = schema_dict['schema_exists_flag']

        if schema_exists_flag == False:

            cursor.execute(
                f"""
                CREATE SCHEMA {schema_name};
                """
            )
            logger.info(f"Schema created: {schema_name}")

    # close cursor
    cursor.close()