import mysql.connector as mydb
from sshtunnel import SSHTunnelForwarder
import pandas.io.sql as psql

from ASP_PASS import ASP_PASS
sshOptions = ASP_PASS.ave_ssh_option()
ave_db = ASP_PASS.ave_db()


def ssh_option():
    ssh = sshOptions["bastion"]
    server = SSHTunnelForwarder(
        (ssh['host'], ssh["port"]),
        ssh_username=ssh['ssh_username'],
        ssh_password=ssh['ssh_password'],
        ssh_pkey=ssh['ssh_private_key'],
        remote_bind_address=(ave_db['host'], 3306)
    )
    return server


def conn_ssh():
    server = ssh_option()
    server.start()
    dbcon = mydb.connect(
        host='localhost',
        user=ave_db["user"],
        password=ave_db["pass"],
        db=ave_db["db"],
        charset='utf8',
        port=server.local_bind_port
    )
    try:
        server.start()
        dbcon.ping(reconnect=True)
        print(f'ssh conn: {dbcon.is_connected()}')
        dbcur = dbcon.cursor()
        return dbcon, dbcur, server
    except Exception as e:
        print(f'error conn_ssh\n{str(e)}')
        dbcon.rollback()
        raise


def select_data(sql):
    try:
        dbcon, dbcur, server = conn_ssh()
        result_data = psql.read_sql(sql, dbcon)
        dbcur.close()
        dbcon.close()

        server.stop()
        return result_data
    except Exception as e:
        print(f'error select_data\n{str(e)}')
        dbcon.rollback()
        raise

def insert_data(sql, list_data):
    try:
        dbcon, dbcur, server = conn_ssh()
        bulk_values = []
        for i, itemRow in enumerate(list_data):
            row = [None if row == 'NULL' else row for row in itemRow]
            if row is not None:
                bulk_values.append(tuple(row))
            else:
                print(f'{i} : {itemRow}')
            # print(f'row: {row}')
            # print(f'bulk_values: {bulk_values}')
            # dbcur.execute(sql, row)
            # dbcon.commit()
            if (i + 1) % 1000 == 0:
                dbcur.executemany(sql, bulk_values)
                dbcon.commit()
                bulk_values = []
            elif i + 1 == len(list_data):
                dbcur.executemany(sql, bulk_values)
                dbcon.commit()
                bulk_values = []
        dbcur.close()
        dbcon.close()

        server.stop()
        print('fin add')
    except Exception as e:
        print(f'error insert_data\n{str(e)}')
        dbcon.rollback()
        raise


if __name__ == '__main__':
    conn_ssh()