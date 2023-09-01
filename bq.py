from google.cloud import bigquery
from utils import config
import pandas as pd

TABLE_DIM = config['bq']['table_dim']
TABLE_TO_MATCH = config['bq']['table_to_match']
TABLE_CRAWL_DATES = config['bq']['table_crawl_dates']

client = bigquery.Client()

def append_to_table(df, table, string_columns=[], client=client):
    table_obj = client.dataset(table.split('.')[0]).table(table.split('.')[1])
    job_config = bigquery.LoadJobConfig(schema=[
        bigquery.SchemaField(string_col, "STRING") for string_col in string_columns])
    errors = client.insert_rows_from_dataframe(table_obj, df, job_config=job_config)
    if errors == []:
        print("Rows appended successfully")
    else:
        print("Encountered errors:", errors)
    return errors

def delete_from_table(column, my_list, table, client=client):
    delete_query = f"""DELETE FROM {table} WHERE {column} IN ({"', '".join(my_list)})"""
    query_job = client.query(delete_query)
    if query_job.errors == []:
        print("Rows deleted successfully")
    else:
        print("Encountered errors:", query_job.errors)
    return query_job.errors

def sql_to_df(query, client=client):
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
        SELECT lower(title) as title FROM {table} ORDER BY n_sugg DESC
    """
    df = sql_to_df(query)
    df['title'] = df['title'].apply(lambda x: x.split("(")[0].strip())
    return df

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
        string_columns=['subreddit']
    )

def save_comment_ids_to_match(comment_ids, table=TABLE_TO_MATCH):
    return append_to_table(
        df=pd.DataFrame(comment_ids, columns = ["subreddit", "comment_id", "comment_timestamp"]),
        table=table,
        string_columns=["subreddit", "comment_id"]
    )

def get_comment_ids_to_match(subreddit, table=TABLE_TO_MATCH):
    return list(sql_to_df(f"""
        SELECT * FROM {table} WHERE subreddit = '{subreddit}' ORDER BY comment_timestamp DESC
    """)['comment_id'])

def remove_comment_ids_to_match(ids, table=TABLE_TO_MATCH):
    return delete_from_table(
        column='comment_id',
        my_list=ids,
        table=table
    )