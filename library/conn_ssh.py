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
        return dbcon, dbcur
    except Exception as e:
        print(f'error {str(e)}')
        dbcon.rollback()
        raise


def select_data(sql):
    dbcon, dbcur = conn_ssh()
    result_data = psql.read_sql(sql, dbcon)
    dbcur.close()
    dbcon.close()
    return result_data


if __name__ == '__main__':
    conn_ssh()