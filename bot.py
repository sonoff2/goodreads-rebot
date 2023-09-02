import praw_wrapper
import bq
import utils
import logging
from utils import config
from formatter import Formatter

class Reader:

    def __init__(self, config=config):
        self.reddit = praw_wrapper.init(config)
        self.subreddit_str = config['reddit']['subreddit']
        self.subreddit = self.reddit.subreddit(self.subreddit_str)
        self.limit = config['reddit']['limit']
        self.last_timestamp = bq.get_last_timestamp(self.subreddit_str)
        self.latest_comments = []

    def read_comments(self):
        latest_comments = []
        for comment in self.subreddit.comments(limit=self.limit):
            if comment.created_utc >= self.last_timestamp:
                latest_comments.append(comment)
            else:
                break

        self.latest_comments = latest_comments

        if latest_comments:
            max_comment_timestamp = max(comment.created_utc for comment in latest_comments)
            bq.update_timestamp(self.subreddit_str, max_comment_timestamp)

        return self

    def save_comments(self):
        filtered_comments = [
            comment for comment in self.latest_comments if "{{" in comment.body and "}}" in comment.body]

        if filtered_comments:
            # Store comment IDs in Firestore
            comment_ids = [[self.subreddit_str, comment.id, comment.created_utc] for comment in filtered_comments]
            bq.save_comment_ids_to_match(comment_ids)

        return True


class Matcher:

    def __init__(self, config):

        # Matching part:
        self.subreddit_str = config['reddit']['subreddit']
        self.comment_ids = bq.get_comment_ids_to_match(subreddit=self.subreddit_str)
        self.books_by_author = bq.get_books_by_author()  # format = {« jk rowling »: [list…], etc}
        self.all_titles = bq.get_all_titles()  # format = list of book titles in DB
        self.series_titles = None  # get it only if needed
        self.min_ratio = config['matching']['min_ratio']
        self.author_min_ratio = config['matching']['author_min_ratio']

        # Replying part:
        self.reddit = praw_wrapper.init(config)


    # FULL FLOW TO TREAT ONE POST

    def process_one_comment(self):
        comment_id = bq.get_comment_ids_to_match(self.subreddit_str)[0] # Most recent comment
        comment = self.reddit.comment(id=comment_id)
        title_matches = self.match_titles(comment.body)
        reply_text = self.build_reply(title_matches)
        self.post_reply(comment, reply_text)

    def match_titles(self, body):
        """
        return the list of title(s) matching the {{string(s)}} in comment body
        :param body:
        :return:
        """
        return [
            self.match_title(s) for s in utils.extract_braces(body)
        ]

    def match_title(self, s):
        title = [(None, 0)]
        if len(s) > 150:
            return [(None, 0)]  # parsing error
        s = str.lower(s.strip())
        if "by" in s: # Maybe the user provided the author:
            title = self.match_author(s)
            if not self.title_is_valid(title): # But maybe it's the book title that contains "by"
                title = self.match_titles_with_by(s)
                if not self.title_is_valid(title): # Or there is a mistake on author, lets drop it
                    title = self.match_all(s.split(" by ")[0])
        if (not self.title_is_valid(title)) or (" by " not in s): # If title was not found, do basic search
            title = self.match_all(s)
        return title

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

    def build_reply(self, title_matches):
        logging.info(f"Building the reply for all matches: {title_matches}")
        reply = "\n\n".join([
            Formatter(title_match=title_match[0], nth=i).format_all() for i, title_match in enumerate(title_matches)
        ])
        return reply

    def post_reply(self, comment, reply_text):
        try:
            posted_reply = comment.reply(reply_text)

            # If reply is successfully posted, remove the comment from Firestore
            if posted_reply:
                bq.remove_comment_ids_to_match(ids=[comment.id])

        except Exception as e:
            print(f"Error posting reply: {e}")

class Bot:

    def __init__(self, config):
        self.reader = Reader(config)
        self.poster = Matcher(config)

    def run_crawling(self):
        self.reader.read_comments()
        self.reader.save_comments()

    def match_and_reply_one(self):
        self.poster.process_one_comment()