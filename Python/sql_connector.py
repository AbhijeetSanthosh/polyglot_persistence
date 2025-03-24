import pyodbc
from config import SQL_CONFIG

def get_sql_connection():
    """Returns a SQL Server connection object."""
    try:
        conn = pyodbc.connect(
            f"DRIVER={SQL_CONFIG['driver']};"
            f"SERVER={SQL_CONFIG['server']};"
            f"DATABASE={SQL_CONFIG['database']};"
            f"UID={SQL_CONFIG['username']};"
            f"PWD={SQL_CONFIG['password']};"
            f"TrustServerCertificate={SQL_CONFIG['trust_server_certificate']};"
            f"Encrypt={SQL_CONFIG['encrypt']};"
        )
        print("✅ SQL Connection successful!")
        return conn
    except pyodbc.Error as e:
        print(f"❌ Connection failed: {e}")
        raise  # Re-raise the exception for the caller to handle


get_sql_connection()