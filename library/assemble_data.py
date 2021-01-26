import pandas as pd
from pytz import timezone
from datetime import datetime

from library import publish_sql


def create_add_unique_id():
    local_data = publish_sql.get_from_local()
    ssh_data = publish_sql.get_from_ssh(['post_name'])

    df_local = local_data.dropna()

    # create unique id
    ssh_url_id = ssh_data['post_name'].values.tolist()
    local_url_id = df_local['url_id'].values.tolist()
    dd_unique_id = list(set(local_url_id) - set(ssh_url_id))

    # create new data with unique id
    df_new = pd.DataFrame(dd_unique_id, columns=['url_id'])
    df_new = pd.merge(df_new, local_data, how='left')
    df_new = df_new.rename(columns={'url_id': 'post_name', 'name': 'post_title'})
    df_new.to_csv('../../jupyter/df_new.csv')
    return df_new


def create_add_product_list(df_new):
    jst_now = datetime.now()
    df_new['post_date'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    df_new['post_date_gmt'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    df_new['post_modified'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    df_new['post_modified_gmt'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")

    df_new['post_author'] = 1
    df_new['post_content'] = '[sc file="item_text"]'
    df_new['post_excerpt'] = '[sc file="item_top"]'
    df_new['post_status'] = "publish"
    df_new['comment_status'] = "open"
    df_new['ping_status'] = "closed"
    df_new['post_parent'] = 0
    df_new['menu_order'] = 0
    df_new['post_type'] = "product"
    df_new['comment_count'] = 0

    df_add = df_new.reindex(columns=['post_author', 'post_date', 'post_date_gmt', 'post_content',
                                     'post_title', 'post_excerpt', 'post_status', 'comment_status',
                                     'ping_status', 'post_name', 'post_modified',
                                     'post_modified_gmt', 'post_parent', 'menu_order', 'post_type',
                                     'comment_count']
                            )

    list_columns = df_add.columns.tolist()
    # print(f'len(list_columns): {len(list_columns)}')
    list_data = df_add.values.tolist()
    return list_columns, list_data


def create_add_attachment_list(df_new):
    target_data = ['id', 'post_name']
    ssh_data = publish_sql.get_from_ssh(target_data)

    jst_now = datetime.now()
    df_new['post_date'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    df_new['post_date_gmt'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    df_new['post_modified'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")
    df_new['post_modified_gmt'] = jst_now.strftime("%Y-%m-%d %H:%M:%S")

    df_new['post_author'] = 77777
    df_new['post_status'] = "inherit"
    df_new['comment_status'] = "open"
    df_new['ping_status'] = "open"
    df_new['menu_order'] = 0
    df_new['post_type'] = "attachment"
    df_new['post_mime_type'] = 'image/jpeg'
    df_new['comment_count'] = 0

    df_new['guid'] = 'img/card/' + df_new['post_name'] + '.jpg'
    df_new = df_new.merge(df_new, ssh_data, on='post_name', how='left')
    df_new = df_new.drop(columns=['post_name'])

    df_add = df_new.reindex(columns=['post_author', 'post_date', 'post_date_gmt',
                                     'post_title',  'post_status', 'comment_status',
                                     'ping_status', 'post_modified', 'post_modified_gmt',
                                     'post_parent', 'menu_order', 'post_type',
                                     'comment_count']
                            )

    list_columns = df_add.columns.tolist()
    # print(f'len(list_columns): {len(list_columns)}')
    list_data = df_add.values.tolist()
    return list_columns, list_data

if __name__ == '__main__':
    create_add_unique_id()