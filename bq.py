from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.schema import SchemaField
from utils import config
import pandas as pd
import logging
from typing import List, Any

TABLE_DIM = config['bq']['table_dim']
TABLE_TO_MATCH = config['bq']['table_to_match']
TABLE_CRAWL_DATES = config['bq']['table_crawl_dates']

if config['flow']['mode'] == 'local':
    credentials = service_account.Credentials.from_service_account_file(
        config['creds']['bq_path'], scopes=["https://www.googleapis.com/auth/cloud-platform"])
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)
else:
    client = bigquery.Client()

def append_to_table(df, table, schema_dic, client=client):
    table_obj = client.dataset(table.split('.')[0]).table(table.split('.')[1])
    schema = [
        SchemaField(name=column, field_type=typ) for (column,typ) in schema_dic.items()
    ]
    errors = client.insert_rows_from_dataframe(table_obj, df, selected_fields=schema)
    if errors == []:
        print("Rows appended successfully")
    else:
        print("Encountered errors:", errors)
    return errors

def delete_from_table(column, my_list, table, client=client):
    delete_query = f"""DELETE FROM {table} WHERE CAST({column} AS STRING) IN ('{"', '".join(my_list)}')"""
    logging.info(f"Running query : {delete_query}")
    query_job = client.query(delete_query)
    if query_job.errors == []:
        print("Rows deleted successfully")
    else:
        print("Encountered errors:", query_job.errors)
    return query_job.errors

def sql_to_df(query, client=client):
    logging.info(f"""Attempting to run the query : "{query.strip()}" """)
    return client.query(query).result().to_dataframe()

def get_books_by_author(table=TABLE_DIM):
    query = f"""
        SELECT lower(last_name) as author, lower(title) as title FROM {table} ORDER BY author ASC, n_sugg DESC
    """
    df = sql_to_df(query)
    df['title'] = df['title'].apply(lambda x: x.split("(")[0].strip())
    return df.groupby('author')['title'].apply(list)

def get_all_titles(table=TABLE_DIM):
    query = f"""
        SELECT lower(title_short) as title_short FROM {table} ORDER BY n_sugg DESC
    """
    return list(sql_to_df(query)['title_short'])

def get_series_titles(table=TABLE_DIM):
    query = f"""
        SELECT lower(title_series) as title_series from {table} WHERE title_series is not null
    """
    return list(sql_to_df(query)['title_series'])

def get_last_timestamp(subreddit, table=TABLE_CRAWL_DATES):
    query = f"""
    SELECT MAX(crawl_timestamp) as timestamp FROM {table} WHERE subreddit = '{subreddit}'
    """
    df = sql_to_df(query)
    return df['timestamp'].max()

def update_timestamp(subreddit, timestamp, table=TABLE_CRAWL_DATES):
    return append_to_table(
        df=pd.DataFrame(data={'subreddit': [subreddit], 'crawl_timestamp': [int(timestamp)]}),
        table=table,
        schema_dic = {'subreddit': 'STRING', 'crawl_timestamp': 'INTEGER'}
    )

def save_post_ids_to_match(post_ids, table=TABLE_TO_MATCH):
    return append_to_table(
        df=pd.DataFrame(post_ids, columns = ["subreddit", "post_id", "post_timestamp", "post_type"]),
        table=table,
        schema_dic={"subreddit": "STRING", "post_id": 'STRING', 'post_timestamp': 'INTEGER', "post_type": 'STRING'}
    )

def get_post_ids_to_match(subreddit, table=TABLE_TO_MATCH):
    return sql_to_df(f"""
        SELECT * FROM {table} WHERE subreddit = '{subreddit}' ORDER BY post_timestamp DESC
    """)[['post_id', 'post_type']].values.tolist()

def remove_post_ids_to_match(ids, table=TABLE_TO_MATCH):
    logging.info(f"Deleting ids {ids} from table {TABLE_TO_MATCH}")
    return delete_from_table(
        column='post_id',
        my_list=ids,
        table=table
    )

def get_book_info(title, is_series, table=TABLE_DIM, order_by='n_sugg'):
    if is_series:
        col_sql = 'title_series'
    else:
        col_sql = 'title_short'
    return sql_to_df(
            f"""SELECT * FROM {table} WHERE LOWER({col_sql}) = '{title}' ORDER BY {order_by} DESC"""
        ).iloc[0].to_dict()
