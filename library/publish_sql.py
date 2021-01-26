import pandas.io.sql as psql

import conn_local, conn_ssh


def get_name_from_local():
    sql = 'SELECT name, url_id FROM creditcard'
    result_data = conn_local.select_data(sql)
    return result_data


def get_name_from_ssh():
    sql = 'SELECT post_name FROM wp_posts WHERE post_name REGEXP "^[0-9]*[a-z][-]*"'
    result_data = conn_ssh.select_data(sql)
    return result_data


if __name__ == '__main__':
    result_data = get_name_from_ssh()
    print(result_data)