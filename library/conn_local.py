import mysql.connector as mydb

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
    except Exception as e:
        print(f'error {str(e)}')
        dbcon.rollback()
        raise
    finally:
        dbcur.close()
        dbcon.close()


if __name__ == '__main__':
    conn_local()