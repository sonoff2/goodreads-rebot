from grbot import bot, utils
import logging
import time
from grbot.utils import config

def sleep(minutes):
    seconds = 60*minutes
    for i in range(int(seconds)):
        time.sleep(1)

def loop():
    while True:
        print("\n\n################## STARTING A LOOP #####################\n\n")
        try:
            process_once()
            sleep(minutes=4)
        except Exception as e:
            print(e)
            logging.info(e)
            sleep(minutes=8)

def process_once():

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

    # Start logging
    utils.setup_logging()

    loop()