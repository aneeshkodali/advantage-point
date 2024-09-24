from .ingest.ingest_main import main as ingest_main
import logging



def main():

    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')


    ingest_main()

if __name__ == "__main__":
    main()