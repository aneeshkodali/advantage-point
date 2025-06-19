import logging
import psycopg2

def create_target_table(
    connection: psycopg2.connect,
    target_database_name: str,
    target_schema_name: str,
    target_table_name: str,
    source_database_name: str,
    source_schema_name: str,
    source_table_name: str
):
    """
    Arguments:
    - connection: SQL connection
    - target_database_name: Database name for target table
    - target_schema_name: Schema name for target table
    - target_table_name: Target table name
    - source_database_name: Database name for source table
    - source_schema_name: Schema name for source table
    - source_table_name: Source table name

    Based on source table columns, creates target table
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # create cursor
    cursor = connection.cursor()

    # generate sql statemet to get source table column names and data types
    source_table_columns_sql = f"""
        WITH
            -- select columns from source table
            COLUMNS_SOURCE AS (
                SELECT
                    CONCAT(COLUMN_NAME, ' ', DATA_TYPE) AS COLUMN_NAME_DATA_TYPE
                FROM {source_database_name}.INFORMATION_SCHEMA.COLUMNS
                WHERE 1=1
                    AND TABLE_SCHEMA = '{source_schema_name}'
                    AND TABLE_NAME = '{source_table_name}'
            ),
            COLUMNS_CONCAT AS (
                -- combine columns into string
                SELECT
                    STRING_AGG(COLUMN_NAME_DATA_TYPE, ', ') AS COLUMN_NAME_DATA_TYPE_AGG
                FROM COLUMNS_SOURCE
            )
        SELECT * FROM COLUMNS_CONCAT
        ;    
    """

    # execute query for source table columns
    # logger.debug(f"Running SQL: {source_table_columns_sql}")
    cursor.execute(source_table_columns_sql)
    column_name_data_type_agg = cursor.fetchone()[0]

    if not column_name_data_type_agg:
        logger.warning("No columns retrieved from source table for target table creation.")


    # generate sql to create target table (includes audit fields)
    create_target_table_sql = f"""
        CREATE TABLE {target_database_name}.{target_schema_name}.{target_table_name}
        (
            {column_name_data_type_agg},
            AUDIT_COLUMN__ACTIVE_FLAG BOOLEAN,
            AUDIT_COLUMN__RECORD_TYPE TEXT,
            AUDIT_COLUMN__START_DATETIME_UTC TIMESTAMP,
            AUDIT_COLUMN__END_DATETIME_UTC TIMESTAMP,
            AUDIT_COLUMN__INSERT_DATETIME_UTC TIMESTAMP,
            AUDIT_COLUMN__UPDATE_DATETIME_UTC TIMESTAMP,
            AUDIT_COLUMN__DELETE_DATETIME_UTC TIMESTAMP
        )
    """

    # execute query for target table creation
    # logger.debug(f"Running SQL: {create_target_table_sql}")
    cursor.execute(create_target_table_sql)
    # logger.info(f"Target table created: {target_database_name}.{target_schema_name}.{target_table_name}")

    # close cursor
    cursor.close()