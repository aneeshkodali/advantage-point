from utils.functions.env.format_env_value import format_env_value
from utils.functions.env.load_env_file import load_env_file
from utils.functions.supabase.create_connection import create_connection
# from utils.functions.supabase.create_databases import create_databases
from utils.functions.supabase.create_schemas import create_schemas
from utils.functions.supabase.query_control_table import query_control_table
import importlib
import logging
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

        # # create table databases
        # logger.info(f"Ensuring databases exist")
        # create_databases(
        #     connection=connection
        # )

        # create table schemas
        logger.info(f"Ensuring schemas exist")
        create_schemas(
            connection=connection
        )

        # query control table
        control_table_record_list = query_control_table(
            connection=connection
        )

        # loop through control table records
        logger.info(f"Looping through control table records: {len(control_table_record_list)}")
        for control_table_dict in control_table_record_list:

            # parse control table record
            source_script_name = control_table_dict['source_script_name']
            target_schema_name = control_table_dict['target_schema_name']
            target_table_name = control_table_dict['target_table_name']

            logger.info(f"Beginning ingestion process for target table: {target_schema_name}.{target_table_name}")

            # extract source data
            try:
                logger.info(f"Retrieving source data using script: {source_script_name}")
                
                source_script = importlib.import_module(source_script_name)
                source_data_df = source_script.main()

             # continue with next record if extract function fails 
            except Exception as e:
                logger.error(f"Exception for {source_script_name}: {e}")
                logger.error(traceback.format_exc())
                continue

    except Exception as e:
        logger.error(f"Error with ingestion process: {e}")
        logger.error(traceback.format_exc())

    # if no data returned, continue with next control table record
    if source_data_df.empty or source_data_df is None:
        logger.warning(f"Extracted data is empty for script {source_script_name}. Skipping.")
        continue

    finally:
        
        # close database connection
        logger.info(f"End of ingestion process")
        connection.close()

if __name__ == '__main__':
    main()