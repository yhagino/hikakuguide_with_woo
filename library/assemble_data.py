import pandas as pd
from pytz import timezone
from datetime import datetime

import publish_sql


def create_add_product_list():
    local_data = publish_sql.get_name_from_local()
    ssh_data = publish_sql.get_name_from_ssh()

    df_local = local_data.dropna()
    df_ssh = ssh_data.copy

    # create unique id
    ssh_url_id = df_ssh['post_name'].values.tolist()
    local_url_id = df_local['url_id'].values.tolist()
    add_url_id = list(set(local_url_id) - set(ssh_url_id))

    # create new data with unique id
    df_new = pd.DataFrame(add_url_id, columns=['url_id'])
    df_new = pd.merge(df_new, local_data, how='left')

    utc_now = datetime.now(timezone('UTC'))
    jst_now = utc_now.astimezone(timezone('Asia/Tokyo'))

    df_new['post_author'] = 1
    df_new['post_date'] = jst_now
    df_new['post_date_gmt'] = utc_now
    df_new['post_content'] = "[sc file='item_text']"
    df_new['post_excerpt'] = "[sc file='item_top']"
    df_new['post_status'] = "publish"
    df_new['comment_status'] = "open"
    df_new['ping_status'] = "closed"
    df_new['post_modified'] = jst_now
    df_new['post_modified_gmt'] = utc_now
    df_new['post_parent'] = 0
    df_new['menu_order'] = 0
    df_new['post_type'] = "product"
    df_new['comment_count'] = 0

    df_add = df_new.rename(columns={'url_id': 'post_title', 'name': 'post_name'})
    df_add = df_add.reindex(columns=['post_author','post_date','post_date_gmt','post_content','post_title','post_excerpt','post_status','comment_status','ping_status','post_name','post_modified','post_modified_gmt','post_parent','menu_order','post_type','post_mime_type','comment_count'])

    list_columns = df_add.columns.tolist()
    list_data = df_add.values.tolist()
    return list_columns, list_data