import os
from google.cloud import storage
import pickle


def init():
    # Initialize the Google Cloud Storage client with automatic credentials
    client = storage.Client()
    return client

# MANAGE LAST TIMESTAMP:

def get_last_timestamp(config, client=None):
    if client is None:
        client = init()

    bucket = client.bucket(config['GCS']['bucket'])
    blob = bucket.blob(config['GCS']["timestamp_file_name"])

    if blob.exists():
        return blob.download_as_text()
    return None


def update_last_timestamp(timestamp, config, client=None):
    if client is None:
        client = init()

    bucket = client.bucket(config['GCS']['bucket'])
    blob = bucket.blob(config['GCS']["timestamp_file_name"])
    blob.upload_from_string(timestamp)


# COMMENTS TO ANALYZE BY MATCHER

def save_comment_ids(comment_ids_list, config, file_name, client):
    bucket = client.bucket(config['GCS']['bucket'])
    blob = bucket.blob(config['GCS'][file_name])

    # Serialize the object to bytes using pickle
    object_bytes = pickle.dumps(comment_ids_list)
    blob.upload_from_string(object_bytes)


def get_comment_ids(config, file_name, client):
    bucket = client.bucket(config['GCS']['bucket'])
    blob = bucket.blob(config['GCS'][file_name])

    object_bytes = blob.download_as_bytes()
    comment_ids_list = pickle.loads(object_bytes)

    return comment_ids_list


def save_comments_todo(comment_ids_list, config, client=None):
    if client is None:
        client = init()
    return save_comment_ids(
        comment_ids_list=comment_ids_list,
        config=config,
        file_name="comment_ids_todo_pkl_name",
        client=client)

def save_comments_done(comment_ids_list, config, client=None):
    if client is None:
        client = init()
    return save_comment_ids(
        comment_ids_list=comment_ids_list,
        config=config,
        file_name="comment_ids_done_pkl_name",
        client=client)

def get_comment_ids_todo(config, client=None):
    if client is None:
        client = init()
    return get_comment_ids(config=config, file_name="comment_ids_todo_pkl_name", client=client)

def get_comment_ids_done(config, client=None):
    if client is None:
        client = init()
    return get_comment_ids(config=config, file_name="comment_ids_done_pkl_name", client=client)