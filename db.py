import os
import pymysql
from dotenv import load_dotenv

load_dotenv()

def get_conn():
    return pymysql.connect(
        host=os.getenv('DB_HOST'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        db=os.getenv('DB_NAME'),
        port=int(os.getenv('DB_PORT')),
        charset='utf8mb4',
        autocommit=True
    )

def fetch_all(query, params=None):
    conn = get_conn()
    with conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def execute_query(query, params=None):
    conn = get_conn()
    with conn:
        with conn.cursor() as cur:
            cur.execute(query, params or ())
            conn.commit()
