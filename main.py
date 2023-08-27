import bot
import firestore
import praw_wrapper
import time
import utils
import logging


def main():

    # Start logging
    utils.setup_logging()

    # Parse arguments and load config
    logging.info("Parsing arguments and loading config")
    args = utils.parse_arguments()
    config = utils.load_config(args)

    # Intiate instances
    reddit = praw_wrapper.init(config)
    db, app = firestore.init()
    grbot = bot.Bot(reddit, db)

    # 1) Crawl comments
    config["last_timestamp"] = firestore.get_last_timestamp()
    comments = grbot.crawl_and_parse(config)

    # Calculate the from_timestamp based on your requirements
    from_timestamp = "XXX"

    while True:
        grbot.run('Bonsai', from_timestamp)  # Replace 'Bonsai' with your desired subreddit name

        # Update the last_timestamp in Firestore
        firestore.update_last_timestamp(db, from_timestamp)



if __name__ == "__main__":
    main()