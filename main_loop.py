from grbot import bot
import logging
import time
from grbot.configurator import config, setup_logging

def sleep(minutes):
    seconds = 60*minutes
    for i in range(int(seconds)):
        time.sleep(1)

def loop():

    # Intiate instances
    my_bot = bot.Bot(config)

    while True:
        print("\n\n################## STARTING A LOOP #####################\n\n")
        try:
            process_once(my_bot)
            logging.info("Started Waiting")
            sleep(minutes=3)
        except Exception as e:
            logging.info(f"ERROR IN LOOP ! {e}")
            sleep(minutes=8)

def process_once(my_bot):

    # 1) Crawl comments
    if config["flow"]["run_reader"]:
        logging.info("Started Crawling")
        my_bot.run_crawling()

    # 2) Answer one
    if config["flow"]["run_poster"]:
        logging.info("Started Matching")
        my_bot.match_and_reply_one()

    # 3) Check scores and remove downvoted
    if config["flow"]["run_check_scores"]:
        logging.info("Started Score Check")
        my_bot.check_scores()

if __name__ == "__main__":
    setup_logging()
    loop()