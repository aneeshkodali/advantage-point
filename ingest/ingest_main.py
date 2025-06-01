from utils.functions.env.format_env_value import format_env_value
from utils.functions.env.load_env_file import load_env_file
from utils.functions.supabase.create_connection import create_connection
from utils.functions.supabase.format_sql_query_results import format_sql_query_results

import logging
import os

def main():

    # configure logging
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s"
    )
    logger = logging.getLogger(__name__)

    # load environment variables
    ENV_FILE_PATH = r'G:\My Drive\Projects\advantage_point\advantage-point\ingest\.env'
    load_env_file(env_path=ENV_FILE_PATH)
    ingestion_database_name = os.getenv("SUPABASE_DATABASE")

    # initialize database connection
    connection = create_connection()
    # initialize cursor
    cursor = connection.cursor()

    # query control table
    control_table_query = f"""
        select
            *
        from {ingestion_database_name}.meta.control_table__web_scripts
        where is_active = True
    """
    cursor.execute(control_table_query)

    control_table_record_list = format_sql_query_results(cursor)

    logger.info(control_table_record_list)

    # close database connection
    cursor.close()
    connection.close()

if __name__ == '__main__':
    main()