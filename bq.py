from google.cloud import bigquery
from google.oauth2 import service_account
from google.cloud.bigquery.schema import SchemaField
from google.api_core.exceptions import Conflict
from utils import config, replace_nan
import pandas as pd
import logging

TABLE_DIM_BOOKS = config['bq']['table_dim_books']
TABLE_DIM_SERIES = config['bq']['table_dim_series']
TABLE_TO_MATCH = config['bq']['table_to_match']
TABLE_CRAWL_DATES = config['bq']['table_crawl_dates']
TABLE_RECO = config['bq']['table_reco']
TABLE_REPLY_LOGS = config['bq']['table_reply_logs']

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
    logging.info(f"Attempting to append df {df} to table {table} with schema {schema}")
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


def overwrite_populate(df, table_id, schema_dic, client=client):

    project_id, dataset_id, table_bq_id = table_id.split('.')
    dataset = bigquery.Dataset(project_id+'.'+dataset_id)

    # Check if table exists and delete if exists
    tables = list(client.list_tables(dataset))
    if table_bq_id in [table.table_id for table in tables]:
        table_ref = client.dataset(dataset_id).table(table_bq_id)
        client.delete_table(table_ref)
        print("Table {}:{} deleted.".format(dataset_id, table_bq_id))

    # Create a new table with the schema
    table = bigquery.Table(f"{project_id}.{dataset_id}.{table_bq_id}")
    table.schema = [bigquery.SchemaField(key, value) for key, value in schema_dic.items()]

    # Create the table in BigQuery
    try:
        table = client.create_table(table)
        print("Created table {}".format(table.table_id))
    except Conflict as error:
        print("Error writing table on BigQuery: ", error)
        return

    try:
        # Trying to write a df on Bigquery
        client.load_table_from_dataframe(df, table).result()
        print(f"{table_id} populated successfully in BigQuery.")

    except Exception as e:
        print(f"Populating failed with error {e}")

    return

def sanitize_for_sql(s):
    return s.replace("'", r"\'")

def sql_to_df(query, client=client):
    logging.info(f"""Attempting to run the query : "{query.strip()}" """)
    return client.query(query).result().to_dataframe()

def get_books_by_author(table=TABLE_DIM_BOOKS, order_by='n_bot'):
    query = f"""
        SELECT lower(last_name) as author, lower(short_title) as title FROM {table} ORDER BY author ASC, {order_by} DESC
    """
    df = sql_to_df(query)
    return df.groupby('author')['title'].apply(list)

def get_all_titles(table=TABLE_DIM_BOOKS, order_by='n_bot'):
    query = f"""
        SELECT lower(short_title) as short_title FROM {table} ORDER BY {order_by} DESC
    """
    return list(sql_to_df(query)['short_title'])

def get_series_titles(table=TABLE_DIM_SERIES):
    query = f"""
        SELECT lower(series_title) as series_title from {table}
    """
    return list(sql_to_df(query)['series_title'])

def get_last_timestamp(subreddit, table=TABLE_CRAWL_DATES):
    query = f"""
    SELECT COALESCE(MAX(crawl_timestamp),0) as timestamp FROM {table} WHERE subreddit = '{subreddit}'
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
        df=pd.DataFrame(post_ids, columns = ["subreddit", "post_id", "post_timestamp", "post_type"]).drop_duplicates(),
        table=table,
        schema_dic={"subreddit": "STRING", "post_id": 'STRING', 'post_timestamp': 'INTEGER', "post_type": 'STRING'}
    )

def get_post_ids_to_match(subreddit, table=TABLE_TO_MATCH, table_already_replied=TABLE_REPLY_LOGS):
    return sql_to_df(f"""
        SELECT T.* FROM {table} T 
        LEFT JOIN {table_already_replied} T2 
            USING (post_id, post_type)
        WHERE TRUE
            AND T2.post_id IS NULL -- Post has not been processed yet
            AND T.subreddit = '{subreddit}' 
        ORDER BY post_timestamp DESC
    """)[['post_id', 'post_type']].values.tolist()

def remove_post_ids_to_match(ids, table=TABLE_TO_MATCH):
    logging.info(f"Deleting ids {ids} from table {TABLE_TO_MATCH}")
    return delete_from_table(
        column='post_id',
        my_list=ids,
        table=table
    )

def get_book_info(title, is_series, order_by='n_bot'):
    if is_series:
        book_info = get_book_info_from_series(series_title=title, order_by=order_by)
    else:
        book_info = get_book_info_not_series(book_title=title, order_by=order_by)
    for col in ['year', 'pages']:
        book_info[col] = replace_nan(book_info[col])
    return book_info

def get_book_info_not_series(book_title, table=TABLE_DIM_BOOKS, order_by='n_bot'):
    return sql_to_df(
            f"""SELECT * FROM {table} WHERE LOWER(short_title) = '{sanitize_for_sql(book_title)}' ORDER BY {order_by} DESC"""
        ).iloc[0].to_dict()

def get_book_info_from_series(
    series_title,
    table_serie=TABLE_DIM_SERIES,
    table_book=TABLE_DIM_BOOKS,
    order_by='n_bot'
):
    return sql_to_df(f"""
            WITH TOP_SERIE AS (
                SELECT series_title FROM {table_serie} WHERE LOWER(series_title) = '{sanitize_for_sql(series_title)}' 
                ORDER BY {order_by} DESC LIMIT 1
            )
            SELECT * FROM {table_book} INNER JOIN TOP_SERIE USING (series_title)
            ORDER BY {order_by} DESC LIMIT 1
        """).iloc[0].to_dict()

def get_top_2_books(grlink, table=TABLE_RECO):
    query = f"""
            SELECT * FROM {table} WHERE source_grlink = '{grlink}'
        """
    df = sql_to_df(query)
    return df.sort_values('top').head(2)

def save_reply_logs(df_to_log, table=TABLE_REPLY_LOGS):
    return append_to_table(
        df=df_to_log,
        table=table,
        schema_dic={"subreddit": "STRING", "post_id": 'STRING', "post_type": 'STRING',
                    "reply_id": 'STRING', "master_grlink": 'STRING', "score": "FLOAT", "author": "STRING"}
    )