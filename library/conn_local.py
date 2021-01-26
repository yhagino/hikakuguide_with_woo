import mysql.connector as mydb
import pandas.io.sql as psql

from ASP_PASS import ASP_PASS
local_db = ASP_PASS.local_db()


def conn_local():
    try:
        dbcon = mydb.connect(
            host='localhost',
            port='3306',
            user=local_db["user"],
            password=local_db["pass"],
            database=local_db["db"],
        )
        dbcon.ping(reconnect=True)
        print(f'local conn: {dbcon.is_connected()}')
        dbcur = dbcon.cursor()
        return dbcon, dbcur
    except Exception as e:
        print(f'error {str(e)}')
        dbcon.rollback()
        raise


def select_data(sql):
    dbcon, dbcur = conn_local()
    result_data = psql.read_sql(sql, dbcon)
    dbcur.close()
    dbcon.close()
    return result_data


if __name__ == '__main__':
    sql = "sql"