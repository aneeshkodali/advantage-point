from warehouse.ingest.ingest_main import main as ingest_main
import logging

def main():

    # set logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    # run ingestion layer
    ingest_main()

if __name__ == "__main__":
    main()