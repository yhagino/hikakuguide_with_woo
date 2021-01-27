from library import conn_local, conn_ssh


def get_from_local():
    sql = 'SELECT name, url_id FROM creditcard'
    local_data = conn_local.select_data(sql)
    return local_data


def get_from_ssh(select_columns):
    sql = f'SELECT {",".join(select_columns)} ' \
          f'FROM wp_posts WHERE post_name REGEXP "^[0-9]*[a-z][-]*"'
    print(sql)
    ssh_data = conn_ssh.select_data(sql)
    return ssh_data


def add_product(list_columns, list_data):
    # list_columns = result_data.columns.unique()
    sql = "".join(["INSERT INTO wp_posts (", ",".join(list_columns), ")",
                   f" VALUES({('%s, ' * len(list_columns)).rstrip(', ')})"])
    print(sql)
    # print(f'len(list_columns): {len(list_columns)}')
    conn_ssh.insert_data(sql, list_data)


if __name__ == '__main__':
    sql = add_product()

    # jst_time = datetime.now()
    # print(jst_time)
