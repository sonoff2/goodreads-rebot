from grbot import bot, utils
import logging
from grbot.utils import config


def main():

    # Start logging
    utils.setup_logging()

    # Intiate instances
    my_bot = bot.Bot(config)

    # 1) Crawl comments
    if config["flow"]["run_reader"]:
        logging.info("Started Crawling")
        my_bot.run_crawling()

    # 2) Answer one
    if config["flow"]["run_poster"]:
        logging.info("Started Matching")
        my_bot.match_and_reply_one()

if __name__ == "__main__":
    main()