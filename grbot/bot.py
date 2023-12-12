from grbot import praw_wrapper, bq, utils, matching
from grbot.configurator import config
from grbot.formatting import Formatter
from grbot.matching import Matcher

import logging
import pandas as pd

class Reader:

    def __init__(self, config=config):
        self.reddit = praw_wrapper.init(config)
        self.subreddit_str = config['reddit']['subreddit']
        self.subreddit = self.reddit.subreddit(self.subreddit_str)
        self.limit = config['reddit']['limit']
        self.last_timestamp = bq.get_last_timestamp(self.subreddit_str)
        self.latest_comments = []
        self.latest_submissions = []

    def read_posts(self):
        latest_comments = []
        for comment in self.subreddit.comments(limit=self.limit):
            if comment.created_utc >= self.last_timestamp:
                latest_comments.append(comment)
            else:
                break
        self.latest_comments = latest_comments

        latest_submissions = []
        for submission in self.subreddit.new(limit=self.limit):
            if submission.created_utc >= self.last_timestamp:
                latest_submissions.append(submission)
            else:
                break
        self.latest_submissions = latest_submissions

        if latest_submissions or latest_comments:
            max_timestamp = max(post.created_utc for post in latest_comments + latest_submissions)
            logging.info(f'Updating timestamp with {max_timestamp}')
            bq.update_timestamp(self.subreddit_str, max_timestamp)

        logging.info(f'Got {len(latest_submissions)} posts and {len(latest_comments)} comments')
        return self

    def save_posts(self):
        filtered_comments = [comment for comment in self.latest_comments
                             if "{{" in comment.body and "}}" in comment.body]
        filtered_submissions = [submission for submission in self.latest_submissions
                                if "{{" in submission.selftext and "}}" in submission.selftext]

        if filtered_comments or filtered_submissions:
            # Store IDs to treat in Big Query
            comment_ids = [[self.subreddit_str, comment.id, comment.created_utc, 'comment']
                           for comment in filtered_comments]
            submission_ids = [[self.subreddit_str, submission.id, submission.created_utc, 'submission']
                              for submission in filtered_submissions]
            bq.save_post_ids_to_match(comment_ids + submission_ids)

        return True


class Poster:

    def __init__(self, config):
        self.reddit = praw_wrapper.init(config)
        self.subreddit_str = config['reddit']['subreddit']

    def get_formatters(self, title_matches, books_requested, all_books_recommended_along):
        return [
            Formatter(
                best_match=best_match,
                nth=i,
                total=len(title_matches),
                book_requested=book_requested,
                books_recommended_info=books_recommended_info
            )
            for i, (best_match, book_requested, books_recommended_info)
            in enumerate(zip(title_matches, books_requested, all_books_recommended_along))
        ]

    def build_reply(self, title_matches, formatter_list, suffix = ""):
        logging.info(f"Building the reply for all matches: {title_matches}")
        #suffix = "\n\n*[Sep-23] I'm a revival bot of goodreads-bot, currently warming up its wires on old posts. Stay tuned for the launch. Bzzzt!*"
        reply = "\n\n---\n".join([formatter.format_all() for formatter in formatter_list]) + suffix
        return reply

    def post_reply(self, post, reply_text):
        try:
            logging.info(f"Answering post {post.id} with text : {reply_text}")
            reply = post.reply(reply_text)
            return reply

        except Exception as e:
            print(f"Error posting reply: {e}")
            return None

    def monitoring_after_reply(self, post, post_type, reply, formatter_list):
        if not reply:
            log_list = [self.subreddit_str, post.id, post_type, None]
        else:
            log_list = [self.subreddit_str, post.id, post_type, reply.id]
        df_to_log = pd.DataFrame([
            log_list + [formatter.book_info["master_grlink"], formatter.score, post.author.name]
            for formatter in formatter_list
        ], columns = ['subreddit', 'post_id', 'post_type', 'reply_id', 'master_grlink', 'score', 'author'])
        bq.save_reply_logs(df_to_log=df_to_log)
        bq.remove_post_ids_to_match(ids=[post.id])
        return

class Bot:

    def __init__(self, config):
        self.subreddit_str = config['reddit']['subreddit']
        self.post_ids = bq.get_post_ids_to_match(subreddit=self.subreddit_str)
        self.reader = Reader(config)
        self.matcher = Matcher(config)
        self.poster = Poster(config)

    def run_crawling(self):
        self.reader.read_posts()
        self.reader.save_posts()

    def match_and_reply_one(self):
        ids = bq.get_post_ids_to_match(self.subreddit_str)
        if len(ids) > 0:
            logging.info("Got one post to match")
            ids = ids[0]
            post_id, post_type = ids[0], ids[1]  # Most recent comment
            if post_type == "comment":
                post = self.reader.reddit.comment(id=post_id)
                books_requested = utils.extract_braces(post.body)[0:config['reddit']['max_search_per_post']]
            elif post_type == "submission":
                post = self.reader.reddit.submission(id=post_id)
                books_requested = utils.extract_braces(post.selftext)[0:config['reddit']['max_search_per_post']]
            else:
                raise ValueError
            title_matches = self.matcher.process_queries(books_requested)
            books_recommended_along = self.matcher.recommend_books(title_matches, k=5)
            formatters = self.poster.get_formatters(title_matches, books_requested, books_recommended_along)
            reply_text = self.poster.build_reply(title_matches, formatters)
            logging.info(f'Posting: {reply_text}')
            # hijack_post = self.reddit.comment(id='jysq9m0')
            # reply = self.post_reply(hijack_post, post.body + '\n########################\n\n' + reply_text)
            reply = self.poster.post_reply(post, reply_text)
            self.poster.monitoring_after_reply(post, post_type, reply, formatters)
        else:
            logging.info("bq.get_post_ids_to_match returned empty string")
