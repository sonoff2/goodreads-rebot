import praw

def init(config):
    reddit = praw.Reddit(
      client_id=config['creds']['reddit_client_id'],
      client_secret=config['creds']['reddit_client_secret'],
       #...
    )
    return reddit