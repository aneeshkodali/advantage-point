from typing import (
    List,
)
import logging
import psycopg2

def update_target_table(
    connection: psycopg2.connect,
    target_database_name: str,
    target_schema_name: str,
    target_table_name: str,
    source_database_name: str,
    source_schema_name: str,
    source_table_name: str,
    unique_column_name_list: List[str]
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
    - unique_column_name_list: Column list that determines uniqueness among rows

    Compare records between source table and target table.
    For a given record in the target table, if there is an updated version of that record in the source table:
    - expire the current target table record
    - insert the updated source table record as a new record in the target table
    """

    # Use a module-specific logger
    logger = logging.getLogger(__name__)

    # create cursor
    cursor = connection.cursor()

    # create alias variables (for use in SQL statements)
    target_alias = 'TGT'
    source_alias = 'SRC'
    compare_alias = 'COMPARE'

    # get list of columns from source table (for use in UPDATE statements)
    source_column_sql = f"""
        SELECT
            COLUMN_NAME
        FROM {source_database_name}.INFORMATION_SCHEMA.COLUMNS
        WHERE 1=1
            AND TABLE_SCHEMA = '{source_schema_name}'
            AND TABLE_NAME = '{source_table_name}'
    """
    cursor.execute(source_column_sql)
    source_column_list = [row[0] for row in cursor.fetchall()]

    # generate lists/strings for unique/nonunique columns
    source_column_str = ',\n'.join(source_column_list)
    source_column_str_w_source_alias = ',\n'.join(
        [
            f"{source_alias}.{col}" for col in source_column_list
        ]
    )
    unique_column_join_str = ' AND '.join([
        f"{target_alias}.{unique_column_name} = {source_alias}.{unique_column_name}"
        for unique_column_name in unique_column_name_list
    ])
    non_unique_column_name_list = list(filter(lambda col: col not in unique_column_name_list, source_column_list))
    non_unique_column_where_clause = '(' + '\nOR '.join(
        [
            f"(COALESCE(CAST({source_alias}.{col} AS TEXT), 'null') != COALESCE(CAST({target_alias}.{col} AS TEXT), 'null'))"
            for col in non_unique_column_name_list
        ]
    ) + ')'
    update_unique_column_where_clause = ' AND '.join([
        f"{target_alias}.{unique_column_name} = {compare_alias}.{unique_column_name}"
        for unique_column_name in unique_column_name_list
    ])

    # create table to store updated rows
    update_database_name = source_database_name
    update_schema_name = source_schema_name
    update_table_name = f"{source_table_name}__update"
    drop_update_table_sql = f"""
        DROP TABLE IF EXISTS {update_database_name}.{update_schema_name}.{update_table_name};
    """
    cursor.execute(drop_update_table_sql)
    create_update_table_sql = f"""
        CREATE TABLE IF NOT EXISTS {update_database_name}.{update_schema_name}.{update_table_name} AS
            WITH
                -- select target table columns
                -- select active records
                -- EXCLUDE audit columns
                {target_alias} AS (
                    SELECT
                        {source_column_str}
                    FROM {target_database_name}.{target_schema_name}.{target_table_name}
                    WHERE AUDIT_COLUMN__ACTIVE_FLAG = TRUE
                ),
                -- select source table columns
                {source_alias} AS (
                    SELECT
                        {source_column_str}
                    FROM {source_database_name}.{source_schema_name}.{source_table_name}
                ),
                -- join source and target rows
                JOINED AS (
                    SELECT
                        {source_column_str_w_source_alias}
                    FROM {target_alias}
                    INNER JOIN {source_alias} ON {unique_column_join_str}
                    WHERE {non_unique_column_where_clause}
                )
            SELECT * FROM JOINED
        ;
    """
    # logger.debug(f"Running SQL: {create_update_table_sql}")
    cursor.execute(create_update_table_sql)

    # handle updates for current records
    update_existing_sql = f"""
        UPDATE {target_database_name}.{target_schema_name}.{target_table_name} AS {target_alias}
        SET
                AUDIT_COLUMN__ACTIVE_FLAG = FALSE,
                AUDIT_COLUMN__END_DATETIME_UTC = CURRENT_TIMESTAMP,
                AUDIT_COLUMN__UPDATE_DATETIME_UTC = CURRENT_TIMESTAMP
            FROM {update_database_name}.{update_schema_name}.{update_table_name} AS {compare_alias}
            WHERE 1=1
                AND {target_alias}.AUDIT_COLUMN__ACTIVE_FLAG = TRUE
                AND {update_unique_column_where_clause}
        ;
    """
    logger.info(f"Expiring current records with updates in target table: {target_database_name}.{target_schema_name}.{target_table_name}")
    # logger.debug(f"Running SQL: {update_existing_sql}")
    cursor.execute(update_existing_sql)

    # handle updates for new records
    update_new_sql = f"""
        INSERT INTO {target_database_name}.{target_schema_name}.{target_table_name}
        ({source_column_str}, AUDIT_COLUMN__ACTIVE_FLAG, AUDIT_COLUMN__RECORD_TYPE, AUDIT_COLUMN__START_DATETIME_UTC, AUDIT_COLUMN__INSERT_DATETIME_UTC)
        SELECT
            {source_column_str},
            TRUE AS AUDIT_COLUMN__ACTIVE_FLAG,
            'update' AS AUDIT_COLUMN__RECORD_TYPE,
            CURRENT_TIMESTAMP AS AUDIT_COLUMN__START_DATETIME_UTC,
            CURRENT_TIMESTAMP AS AUDIT_COLUMN__INSERT_DATETIME_UTC
        FROM {update_database_name}.{update_schema_name}.{update_table_name}
        ;
    """
    logger.info(f"Inserting updated records in target table: {target_database_name}.{target_schema_name}.{target_table_name}")
    # logger.debug(f"Running SQL: {update_new_sql}")
    cursor.execute(update_new_sql)

    # drop the update table
    # logger.debug(f"Running SQL: {drop_update_table_sql}")
    cursor.execute(drop_update_table_sql)

    # close cursor
    cursor.close()