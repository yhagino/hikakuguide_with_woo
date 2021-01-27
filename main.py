from library import assemble_data, publish_sql

if __name__ == '__main__':
    df_new = assemble_data.create_add_unique_id()
    list_columns, list_data = assemble_data.create_add_product_list(df_new)
    publish_sql.add_product(list_columns, list_data)
    list_columns, list_data = assemble_data.create_add_attachment_list(df_new)
    publish_sql.add_product(list_columns, list_data)
