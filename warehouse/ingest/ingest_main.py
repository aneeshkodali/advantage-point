from .ingest_tournaments import main as ingest_tournaments

def main():

    print("Starting data ingestion.")

    ingest_tournaments()

    print("Data ingestion completed.")

if __name__ == "__main__":
    main()