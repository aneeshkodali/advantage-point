from utils.functions.env.load_env_file import load_env_file
from utils.functions.supabase.create_connection import create_connection
from utils.functions.version_control.get_current_branch import get_current_branch

import logging

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

    git_branch = get_current_branch()
    logger.info(f"Branch: {git_branch}")

    # close database connection
    db_connection.close()

if __name__ == '__main__':
    main()