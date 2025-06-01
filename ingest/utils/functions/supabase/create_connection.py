import os
import psycopg2

def create_connection():
    """
    Create sql connection
    """

    # create connection
    conn = psycopg2.connect(
        dbname=os.getenv('SUPABASE_DATABASE'),
        user=os.getenv('SUPABASE_USER'),
        password=os.getenv('SUPABASE_PASSWORD'),
        host=os.getenv('SUPABASE_HOST'),
        port=os.getenv('SUPABASE_PORT')
    )

    # create cursor
    cursor = conn.cursor()

    # allow write operations
    conn.autocommit = True
    cursor.execute("SET session characteristics AS transaction READ WRITE;")
    cursor.execute("SET default_transaction_read_only = 'off';")

    # close cursor
    cursor.close()
    
    return conn