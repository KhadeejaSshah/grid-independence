import yaml
import psycopg2
from psycopg2 import pool

_connection_pool = None

def get_db_connection():
    """
    Get a connection from the connection pool.
    """
    global _connection_pool
    if _connection_pool is None:
        with open("conf.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        db_config = config.get("postgresql", {})
        
        _connection_pool = pool.SimpleConnectionPool(
            1, 10,
            user=db_config.get("user"),
            password=db_config.get("password"),
            host=db_config.get("host"),
            port=db_config.get("port"),
            database=db_config.get("dbname")
        )
    
    return _connection_pool.getconn()

def release_db_connection(conn):
    """
    Release a connection back to the pool.
    """
    global _connection_pool
    if _connection_pool:
        _connection_pool.putconn(conn)

def close_db_pool():
    """
    Close all connections in the pool.
    """
    global _connection_pool
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
