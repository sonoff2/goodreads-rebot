from grbot import praw_wrapper, bq, utils
from grbot.configurator import config
from grbot.formatting import Formatter
from grbot.matching import Matcher

import logging
import pandas as pd

class Reader:

    def __init__(self, config=config):
        self.reddit = praw_wrapper.init(config)
        self.subreddits = {sub: self.reddit.subreddit(sub) for sub in config['reddit']['subreddits']}
        self.limit = config['reddit']['limit']
        self.last_timestamps = bq.get_last_timestamps(list(self.subreddits.keys()))
        self.latest_comments = []
        self.latest_submissions = []

    def read_posts(self):

        for sub in list(self.subreddits.keys()):
            self.read_posts_from(sub)

        if self.latest_submissions or self.latest_comments:
            new_timestamps = pd.DataFrame(
                data = [[post.subreddit.display_name, post.created_utc]
                         for post in (self.latest_submissions + self.latest_comments)],
                columns = ["subreddit", "crawl_timestamp"]
            ).groupby('subreddit')['crawl_timestamp'].max().reset_index()
            logging.info(f'Updating timestamp with {new_timestamps}')
            bq.update_timestamps(new_timestamps)

            logging.info(f'Got {len(self.latest_submissions)} posts and {len(self.latest_comments)} comments')

            self.latest_comments, self.latest_submissions = [], []


    def read_posts_from(self, sub):
        latest_comments = []
        for comment in self.subreddits[sub].comments(limit=self.limit):
            if comment.created_utc >= self.last_timestamps[sub]:
                latest_comments.append(comment)
            else:
                break
        self.latest_comments += latest_comments

        latest_submissions = []
        for submission in self.subreddits[sub].new(limit=self.limit):
            if submission.created_utc >= self.last_timestamps[sub]:
                latest_submissions.append(submission)
            else:
                break
        self.latest_submissions += latest_submissions

        return self

    def save_posts(self):
        filtered_comments = [comment for comment in self.latest_comments if utils.comment_triggers(comment)]
        filtered_submissions = [submi for submi in self.latest_submissions if utils.comment_triggers(submi)]

        if filtered_comments or filtered_submissions:
            # Store IDs to treat in Big Query
            comment_ids = [[comment.subreddit.display_name, comment.id, comment.created_utc, 'comment']
                           for comment in filtered_comments]
            submission_ids = [[submission.subreddit.display_name, submission.id, submission.created_utc, 'submission']
                              for submission in filtered_submissions]
            bq.save_post_ids_to_match(comment_ids + submission_ids)

        return True


class Poster:

    def __init__(self, config):
        self.reddit = praw_wrapper.init(config)
        self.subreddit_str = config['reddit']['subreddits'] ### WARNING

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
        self.subreddits_str = config['reddit']['subreddits']
        if config['flow']['run_reader']:
            self.reader = Reader(config)
        if config['flow']['run_matcher']:
            self.post_ids = bq.get_post_ids_to_match(subreddits=self.subreddits_str)
            self.matcher = Matcher(config)
        if config['flow']['run_poster']:
            self.poster = Poster(config)

    def check_scores(self, comment_limit=50, karma_threshold=1):
        """Check recently posted comments and delete if the karma score is below given threshold."""
        logging.info("Checking comment scores...")

        for posted_comment in self.poster.reddit.user.me().comments.new(limit=comment_limit):
            if posted_comment.score < karma_threshold:
                posted_comment.delete()
                url = posted_comment.permalink.replace(posted_comment.id, posted_comment.parent_id[3:])
                logging.info("Downvoted comment removed: https://www.reddit.com" + url)

    def run_crawling(self):
        self.reader.read_posts()
        self.reader.save_posts()

    def match_and_reply_one_by_sub(self):
        for post_to_match in bq.get_post_ids_to_match(self.subreddits_str):
            _, post_id, post_type = post_to_match
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
