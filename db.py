import os
import pymysql
from dotenv import load_dotenv
import logging

load_dotenv()

def get_connection():
    return pymysql.connect(
        host=os.getenv("DB_HOST"),
        user=os.getenv("DB_USER"),
        password=os.getenv("DB_PASSWORD"),
        db=os.getenv("DB_NAME"),
        port=int(os.getenv("DB_PORT", "3306")),
        charset='utf8mb4',
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True
    )

def fetch_all(query, params=None):
    conn = get_connection()
    with conn:
        with conn.cursor(pymysql.cursors.DictCursor) as cur:
            cur.execute(query, params or ())
            return cur.fetchall()

def execute_query(query, params=None, fetch=False):
    conn = get_connection()
    cursor = conn.cursor(pymysql.cursors.DictCursor)  # ✅ DictCursor 적용됨

    try:
        cursor.execute(query, params or ())

        if fetch:
            return cursor.fetchall()  # SELECT 쿼리 결과 반환
        else:
            return cursor.lastrowid   # INSERT 시 생성된 id 반환

    except Exception as e:
        conn.rollback()
        logging.error(f"쿼리 실행 실패: {e}")
        return None

    finally:
        cursor.close()
        conn.close()
