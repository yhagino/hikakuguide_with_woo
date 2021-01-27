import pandas as pd
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
    df_new = pd.merge(df_new, ssh_data, how='left')
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


def create_add_terms_list() -> 'pd.DataFrame':
    ssh_data = pd.DataFrame()  # todo get from ssh
    kkk_split = pd.DataFrame()  # todo get from local.kkk
    local_split = pd.DataFrame()  # todo get from local.credit
    # ssh_data = ssh_data[['ID', 'post_name']]
    """ kkk_split = data_all_kkk[
    ['url_id', 'visa', 'master', 'jcb', 'amex', 'pointChangeToTpoint', 'pointChangeToRakuten',
    'pointChangeToPonta','pointChangeToDpoint', 'pointChangeToNanaco', 'pointChangeToWaon', 'pointChangeToYodobashi',
    'pointChangeToJoshin', 'pointChangeToBigpoint', 'pointChangeToAna', 'pointChangeToJal', 'eMoneyId', 'eMoneyQuickpay',
    'eMoneyEdy', 'eMoneyWaon', 'eMoneyNanaco', 'eMoneySuica', 'eMoneyPasmo']].copy()"""
    kkk_split = kkk_split.fillna(0)
    kkk_split.iloc[:, 1:] = kkk_split.iloc[:, 1:].astype(int)
    """local_split = data_all_local[
    ['name', 'shinsa', 'kind', 'age_10', 'age_20', 'age_30', 'age_40', "_fee", 'issue_days_id', 'has_insurance_internal_id',
    'has_insurance_oversea_id', 'etc_id', 'url_id']]
    """
    local_split.loc[
        local_split['name'].str.contains('Gold') |
        local_split['name'].str.contains('gold') |
        local_split['name'].str.contains('ゴールド'), 'gold'] = 1
    local_split.loc[
        local_split['name'].str.contains('Platinum') |
        local_split['name'].str.contains('プラチナ'), 'platinum'] = 1
    local_split = local_split.fillna(0)

    df_defer = pd.merge(ssh_data, local_split, how='left')
    df_defer = pd.merge(df_defer, kkk_split, how='left')

    df_new = pd.DataFrame([[0,0,0]],columns=['object_id', 'term_taxonomy_id', 'term_order'])

    id_list = list(ssh_data['ID'].unique())
    for i in id_list:
        if df_defer.loc[df_defer['ID']==i, 'kind'].values[0] in '4':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 37, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 52, 'term_order': 0}, ignore_index=True)
        elif df_defer.loc[df_defer['ID']==i, 'kind'].values[0] in '2':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 29, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 50, 'term_order': 0}, ignore_index=True)
        elif df_defer.loc[df_defer['ID']==i, 'kind'].values[0] in '3':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 49, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 51, 'term_order': 0}, ignore_index=True)
        elif df_defer.loc[df_defer['ID']==i, 'kind'].values[0] in '5':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 62, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 63, 'term_order': 0}, ignore_index=True)
        elif df_defer.loc[df_defer['ID']==i, 'kind'].values[0] in '6':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 60, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 64, 'term_order': 0}, ignore_index=True)
        elif df_defer.loc[df_defer['ID']==i, 'kind'].values[0] in '7':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 61, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 65, 'term_order': 0}, ignore_index=True)
        else:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 14, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 28, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 30, 'term_order': 0}, ignore_index=True)

        if df_defer.loc[df_defer['ID']==i, '_fee'].values[0] == 'free':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 31, 'term_order': 0}, ignore_index=True)
        elif df_defer.loc[df_defer['ID']==i, '_fee'].values[0] == 'free_first_year':
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 36, 'term_order': 0}, ignore_index=True)

        if df_defer.loc[df_defer['ID']==i, 'etc_id'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 32, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'issue_days_id'].values[0] < 4:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 33, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'shinsa'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 35, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'has_insurance_internal_id'].values[0] > 0:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 39, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'has_insurance_oversea_id'].values[0] > 0:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 40, 'term_order': 0}, ignore_index=True)

        if df_defer.loc[df_defer['ID']==i, 'pointChangeToJal'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 38, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToAna'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 34, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToTpoint'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 41, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToRakuten'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 42, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToPonta'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 43, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToDpoint'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 44, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToNanaco'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 45, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToWaon'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 46, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'gold'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 47, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'platinum'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 48, 'term_order': 0}, ignore_index=True)

        if df_defer.loc[df_defer['ID']==i, 'age_10'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 56, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'age_20'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 53, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'age_30'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 54, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'age_40'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 55, 'term_order': 0}, ignore_index=True)

        if df_defer.loc[df_defer['ID']==i, 'visa'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 66, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'master'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 67, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'jcb'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 68, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'amex'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 69, 'term_order': 0}, ignore_index=True)
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 70, 'term_order': 0}, ignore_index=True)

        if df_defer.loc[df_defer['ID']==i, 'pointChangeToJoshin'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 71, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToYodobashi'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 72, 'term_order': 0}, ignore_index=True)
        if df_defer.loc[df_defer['ID']==i, 'pointChangeToBigpoint'].values[0] == 1:
            df_new = df_new.append({'object_id': i, 'term_taxonomy_id': 73, 'term_order': 0}, ignore_index=True)
    return df_new

if __name__ == '__main__':
    create_add_unique_id()