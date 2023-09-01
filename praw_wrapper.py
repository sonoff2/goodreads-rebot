import praw

def init(config):
    reddit = praw.Reddit(
        client_id=config['creds']['reddit_client_id'],
        client_secret=config['creds']['reddit_client_secret'],
        user_agent=config['creds']['reddit_user_agent'],
        username=config['creds']['reddit_username'],
        password=config['creds']['reddit_password'],
    )
    return reddit