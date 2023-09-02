import bq
import re
import logging
from utils import config

class Formatter:
    def __init__(self, title_match, nth):
        logging.info(f"Received title_match : {title_match}")
        self.title = str(title_match[0]).rstrip("#")
        self.nth = nth
        self.score = int(title_match[1])
        if self.title is not None:
            self.book_info = bq.get_book_info(self.title)

    def format_link(self):
        title = self.book_info["title"]
        url = self.book_info["grlink"]
        nth = self.nth + 1
        score = self.score

        if score < config['matching']['min_ratio']:
            return f"_Match #{nth} ({score}% = BAD) => '{title}' was ignored_"
        else:
            return f"**Match #{nth} ({score}%): [{title}]({url})**"

    def format_header(self):
        pages = self.book_info["pages"]
        year = self.book_info["year"]
        authors = self.book_info["author_list"].replace("'", "").replace("[", "").replace("]", "")

        return "^(By: %s | %s pages | Published: %s)" % (authors, pages or "?", year or "?")

    def format_description(self):
        description = self.book_info["summary"]
        if description is None:
            return ""
        description = re.sub('<.*?>', '', description.replace("<br />", "\n"))
        chunks = [">>" + chunk for chunk in description.split("\n") if len(chunk) > 2]
        return "\n".join(chunks)

    def format_book_footer(self):
        n_sugg = self.book_info['n_sugg']
        s = "s" if n_sugg > 1 else ""
        return "^(This book has been suggested %s time%s)" % (n_sugg, s)

    def format_all(self):
        if self.score < config['matching']['min_ratio']:
            return self.format_link()
        else:
            return '\n\n'.join([
            self.format_link(),
            self.format_header(),
            self.format_description(),
            self.format_book_footer()
        ])
