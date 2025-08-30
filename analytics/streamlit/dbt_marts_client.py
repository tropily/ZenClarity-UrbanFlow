
import os
import pandas as pd

TARGET = os.getenv("TARGET_WAREHOUSE", "redshift").lower()
DBT_MART_SCHEMA = os.getenv("DBT_MART_SCHEMA", "dev_viper_mart")

def _rs_conn():
    import psycopg2
    return psycopg2.connect(
        host=os.environ["REDSHIFT_HOST_DEV"],
        dbname=os.environ["REDSHIFT_DB_DEV"],
        user=os.environ["REDSHIFT_USER_DEV"],
        password=os.environ["REDSHIFT_PASSWORD_DEV"],
        port=int(os.getenv("REDSHIFT_PORT", "5439")),
        connect_timeout=10,
    )

def _sf_conn():
    import snowflake.connector
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.getenv("SNOWFLAKE_SCHEMA", DBT_MART_SCHEMA),
        client_session_keep_alive=False,
    )

def run_query(sql: str) -> pd.DataFrame:
    if TARGET == "redshift":
        with _rs_conn() as conn:
            return pd.read_sql(sql, conn)
    elif TARGET == "snowflake":
        conn = _sf_conn()
        try:
            cur = conn.cursor()
            cur.execute(sql)
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
            return pd.DataFrame(rows, columns=cols)
        finally:
            try: cur.close()
            except Exception: pass
            conn.close()
    else:
        raise ValueError(f"Unsupported TARGET_WAREHOUSE={TARGET!r}")

