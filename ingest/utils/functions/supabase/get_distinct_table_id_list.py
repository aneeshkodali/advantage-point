from typing import(
    Dict,
    List,
)
from utils.functions.supabase.format_sql_query_results import format_sql_query_results
import psycopg2

def get_distinct_table_id_list(
    connection: psycopg2.connect,
    database_name: str,
    schema_name: str,
    table_name: str,
    id_column_name_list: List[str],
    where_clause_list: List[str] = ['1 = 1']
) -> List[Dict]:
    """
    Arguments:
    - conn: SQL connection
    - database_name: Database name
    - schema_name: Schema name
    - table_name: Table name
    - id_column_name_list: List of column names
    - where_clause_list: List of WHERE clause strings

    Returns column values as a list
    """

    # create cursor
    cursor = connection.cursor()

    # create sql-like strings from list
    id_column_name_join = ', '.join(id_column_name_list)
    where_clause_join = ' AND '.join([f"({where_clause})" for where_clause in where_clause_list])

    # create sql statement to retrieve records for columns
    select_sql = f"""
        SELECT DISTINCT
            {id_column_name_join}
        FROM {database_name}.{schema_name}.{table_name}
        WHERE {where_clause_join}
    """
    logging.info(f"Running select statement: {select_sql}")

    try:
    
        # store results as list of dicts
        cursor.execute(select_sql)
        select_list = format_sql_query_results(
            cursor=cursor
        )

        return select_list

    except Exception as e:
        logging.info(f"Error executing statement: {e}")
        # roll back transaction to reset its state
        connection.rollback()
        return []
    
    finally:
        # close cursor
        cursor.close()