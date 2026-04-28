import psycopg

def get_connection():
    conninfo = "host=127.0.0.1 port=5432 dbname=app_db user=app_user password=app_pass"
    return psycopg.connect(conninfo)