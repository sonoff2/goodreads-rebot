import praw_wrapper
import bq
import utils
import logging
from utils import config
from formatter import Formatter
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


class Matcher:

    def __init__(self, config):

        # Matching part:
        self.subreddit_str = config['reddit']['subreddit']
        self.post_ids = bq.get_post_ids_to_match(subreddit=self.subreddit_str)
        self.books_by_author = bq.get_books_by_author()  # format = {« jk rowling »: [list…], etc}
        self.all_titles = bq.get_all_titles()  # format = list of book titles in DB
        self.series_titles = bq.get_series_titles()
        self.min_ratio = config['matching']['min_ratio']
        self.author_min_ratio = config['matching']['author_min_ratio']

        # Replying part:
        self.reddit = praw_wrapper.init(config)


    # FULL FLOW TO TREAT ONE POST

    def process_one_post(self):
        ids = bq.get_post_ids_to_match(self.subreddit_str)
        if len(ids) > 0:
            ids = ids[0]
            post_id, post_type = ids[0], ids[1] # Most recent comment
            if post_type == "comment":
                post = self.reddit.comment(id=post_id)
                title_matches = self.match_titles(post.body)
            elif post_type == "submission":
                post = self.reddit.submission(id=post_id)
                title_matches = self.match_titles(post.selftext)
            else:
                raise ValueError
            formatters = self.get_formatters(title_matches)
            reply_text = self.build_reply(title_matches, formatters)
            logging.info(f'Posting: {reply_text}')
            reply = self.post_reply(post, reply_text)
            self.monitoring_after_reply(post, post_type, reply, formatters)

    def match_titles(self, body):
        """
        return the list of title(s) matching the {{string(s)}} in comment body
        :param body:
        :return:
        """
        return [
            self.match_title(s) for s in utils.extract_braces(body)[0:10] # 10 matches max par post to avoid flood
        ]

    def match_title(self, s):

        title = [(None, 0)]
        if len(s) > 150:
            return title  # parsing error
        s = str.lower(s.strip())

        # Case 1 : Series
        serie_title = self.match_series(s) # lookup series first

        # Case 2 : not a Series
        if "by" in s: # Maybe the user provided the author:
            title = self.match_author(s)
            if not self.title_is_valid(title): # But maybe it's the book title that contains "by"
                title = self.match_titles_with_by(s)
                if not self.title_is_valid(title): # Or there is a mistake on author, lets drop it
                    title = self.match_all(s.split(" by ")[0])
        if (not self.title_is_valid(title)) or (" by " not in s): # If title was not found, do basic search
            title = self.match_all(s)
        # Not a series so False:
        non_serie_title = [(*t, False) for t in title]

        # Finally return the best match between Series or not
        return  [max(serie_title + non_serie_title, key=lambda x: x[1])]

    def match_series(self, s):
        with_by = utils.top_k_matches_list(s.split(' by ')[0], self.series_titles, 1, func="full")
        without_by = utils.top_k_matches_list(s, self.series_titles, 1, func="full")
        return [(*max(with_by + without_by, key=lambda x: x[1]), True)] # True for "is_series"

    def title_is_valid(self, result):
        try:
            return result[0][1] > self.min_ratio
        except:
            print("Problem validating title result: ", result)
            return False

    def match_author(self, s):
        s_split = s.rsplit(' by ', 1)
        # Last word of the string is supposedly the author last name:
        last_name = next(word for word in reversed(s_split[1].split()) if len(word) >= 2)
        # Get the closest authors in base (the user may have done a typo)
        closest_authors = [x[0].rstrip("#") for x in
            utils.top_k_matches_list(last_name, list(self.books_by_author.keys()), k=3, func='full')
                           if x[1] > self.author_min_ratio]
        # Find the closest book title in their books:
        return utils.top_k_matches_dic(s_split[0], self.books_by_author, closest_authors, k=1, func="full")

    def match_titles_with_by(self, s):
        return utils.top_k_matches_list(s, [title for title in self.all_titles if "by" in title], 1, func="full")

    def match_all(self, s):
        return utils.top_k_matches_list(s, self.all_titles, 1, func="full")

    def get_formatters(self, title_matches):
        return [
            Formatter(title_match=title_match[0], nth=i, total=len(title_matches))
            for i, title_match in enumerate(title_matches)
        ]

    def build_reply(self, title_matches, formatter_list):
        logging.info(f"Building the reply for all matches: {title_matches}")
        suffix = "\n\n*[Sep-23] I'm a revival bot of goodreads-bot, currently warming up its wires on old posts. Stay tuned for the launch. Bzzzt!*"
        reply = "\n---\n".join([formatter.format_all() for formatter in formatter_list])+suffix
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
            log_list + [formatter.book_info["master_grlink"], formatter.score, post.author]
            for formatter in formatter_list
        ], columns = ['subreddit', 'post_id', 'post_type', 'reply_id', 'master_grlink', 'score', 'author'])
        bq.save_reply_logs(df_to_log=df_to_log)
        bq.remove_post_ids_to_match(ids=[post.id])
        return

class Bot:

    def __init__(self, config):
        self.reader = Reader(config)
        self.poster = Matcher(config)

    def run_crawling(self):
        self.reader.read_posts()
        self.reader.save_posts()

    def match_and_reply_one(self):
        self.poster.process_one_post()