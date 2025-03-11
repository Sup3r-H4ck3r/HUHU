from psycopg2 import pool
from config.config import DATABASE_URL

db_pool = pool.ThreadedConnectionPool(minconn=1, maxconn=10, dsn=DATABASE_URL)

def get_db_connection():
    try:
        return db_pool.getconn()
    except Exception as e:
        print(f"Database connection error: {e}")
        return None

def release_db_connection(conn):
    """Trả lại kết nối về pool sau khi sử dụng."""
    if conn:
        db_pool.putconn(conn)
