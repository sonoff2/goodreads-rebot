import praw_wrapper
import firestore
import utils

class Reader:

    def __init__(self, config):
        self.reddit = praw_wrapper.init(config)
        self.subreddit = self.reddit.subreddit(config['reddit']['subreddit'])
        self.limit = config['reddit']['limit']
        self.last_timestamp = firestore.get_last_timestamp()
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
            firestore.update_last_timestamp(max_comment_timestamp, db=None)

        return self

    def save_comments(self):
        filtered_comments = [
            comment for comment in self.latest_comments if "{{" in comment.body and "}}" in comment.body]

        if filtered_comments:
            # Store comment IDs in Firestore
            comment_ids = [comment.id for comment in filtered_comments]
            firestore.save_comment_ids(comment_ids)

        return True


class Poster:

    def __init__(self, config):
        self.reddit = praw_wrapper.init(config)
        self.comment_ids = firestore.get_comment_ids()
        self.all_titles_dict = firestore.get_all_titles()

    # FULL FLOW TO TREAT ONE POST
    def process_one_comment(self, comment_id):
        comment = self.reddit.comment(id=comment_id)
        title_matches = self.match_titles(comment)
        reply_text = self.build_reply(title_matches)
        self.post_reply(comment, reply_text)

    # MODULES:
    def match_titles(self, comment):
        title_matches = []

        for sub_part in utils.extract_braces(comment.body):
            closest_match = None
            max_ratio = -1

            for title_id, title_text in self.all_titles_dict.items():
                if max_ratio < 98:
                    ratio = utils.partial_ratio(sub_part, title_text)
                    if ratio > max_ratio:
                        max_ratio = ratio
                        closest_match = {"title_id": title_id, "title_text": title_text, "match_ratio": ratio}

            title_matches.append(closest_match)

        return title_matches

    def build_reply(self, title_matches):
        return str(title_matches)

    def post_reply(self, comment, reply_text):
        try:
            posted_reply = comment.reply(reply_text)

            # If reply is successfully posted, remove the comment from Firestore
            if posted_reply:
                self.remove_comment_from_db(comment.id)

        except Exception as e:
            print(f"Error posting reply: {e}")



    def remove_comment_from_db(self, comment_id):
        # Remove the comment document from Firestore
        db = firestore.client()
        db.collection("title_matches").document(comment_id).delete()

class Bot:

    def __init__(self, reddit, db):
        self.reddit = reddit
        self.db = db

        self.reader = Reader(reddit)
        self.matcher = Matcher()
        self.poster = Poster(reddit, db)

    def crawl(self, config):


    def parse

    def run(self):
        comments = self.reader.read_comments()
        matches = self.matcher.match_titles(comments)

    for comment, match in matches:
        self.poster.post_reply(comment, match)