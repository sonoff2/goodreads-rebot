import bot
import utils
import logging
from utils import config


def main():

    # Start logging
    utils.setup_logging()

    # Intiate instances
    grbot = bot.Bot(config)

    # 1) Crawl comments
    if config["flow"]["run_reader"]:
        logging.info("Started Crawling")
        grbot.run_crawling()

    # 2) Answer one
    if config["flow"]["run_poster"]:
        logging.info("Started Matching")
        grbot.match_and_reply_one()

if __name__ == "__main__":
    main()