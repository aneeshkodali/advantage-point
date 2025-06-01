from utils.functions.env.format_env_value import format_env_value
from utils.functions.env.load_env_file import load_env_file
from utils.functions.supabase.create_connection import create_connection
from utils.functions.supabase.query_control_table import query_control_table
import logging
import os
import traceback

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

    # initialize database connection
    connection = create_connection()

    logger.info(f"Starting ingestion process")

    try:

        # query control table
        control_table_record_list = query_control_table(
            connection=connection
        )

        logger.info(control_table_record_list)

    except Exception as e:
        logger.error(f"Error with ingestion process: {e}")
        logger.error(traceback.format_exc())

    finally:
        
        # close database connection
        logger.info(f"End of ingestion process")
        connection.close()

if __name__ == '__main__':
    main()