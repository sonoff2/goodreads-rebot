from grbot import bot
import logging
from grbot.configurator import config, setup_logging


def main():

    # Start logging
    setup_logging()

    # Intiate instances
    my_bot = bot.Bot(config)

    # 1) Crawl comments
    if config["flow"]["run_reader"]:
        logging.info("Started Crawling")
        my_bot.run_crawling()

    # 2) Answer one
    if config["flow"]["run_poster"]:
        logging.info("Started Matching")
        my_bot.match_and_reply_one_by_sub()

    # 3) Check scores and remove downvoted
    if config["flow"]["run_check_scores"]:
        logging.info("Started Score Check")
        my_bot.check_scores()

if __name__ == "__main__":
    main()