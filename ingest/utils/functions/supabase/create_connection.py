import psycopg2

def create_connection(
    database_name: str,
    user_name: str,
    user_password: str,
    host_name: str,
    port_number: int
) -> psycopg2.connect:
    """
    Arguments:
    - database_name: Database name
    - user_name: User name
    - password: User password
    - host_name: Host name
    - port_number: Port number
    Create sql connection
    """

    # initialize values
    conn = None
    cursor = None

    try:

        # create connection
        conn = psycopg2.connect(
            dbname=database_name,
            user=user_name,
            password=user_password,
            host=host_name,
            port=port_number
        )

        # create cursor
        cursor = conn.cursor()

        # allow write operations
        conn.autocommit = True
        cursor.execute("SET session characteristics AS transaction READ WRITE;")
        cursor.execute("SET default_transaction_read_only = 'off';")

    except Exception as e:
        print(f"Database error: {e}")
        if conn:
            conn.close()
        raise
        
    finally:
        if cursor:
            cursor.close()
    
    return conn
    
    # close cursor
    cursor.close()
    
    return conn