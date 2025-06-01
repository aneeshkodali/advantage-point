from utils.functions.env.format_env_value import format_env_value
from utils.functions.env.load_env_file import load_env_file
from utils.functions.supabase.create_connection import create_connection

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

    # initialize database connection
    db_connection = create_connection()

    # close database connection
    db_connection.close()

if __name__ == '__main__':
    main()