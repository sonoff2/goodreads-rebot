import bq
import ast
import logging
from utils import config
import itertools
import textwrap

class Formatter:
    def __init__(self, title_match, nth, total):
        logging.info(f"Received title_match : {title_match}")
        self.title = str(title_match[0]).rstrip("#")
        self.nth = nth
        self.total = total
        self.score = int(title_match[1])
        self.is_series = title_match[3]
        if self.title is not None:
            self.book_info = bq.get_book_info(self.title, self.is_series)
            print(self.book_info)

    def build_long_title(self):
        if hasattr(self, "book_info"):
            if self.book_info["series_title"] is not None:
                return "{} ({} #{})".format(
                    self.book_info['short_title'],
                    self.book_info['series_title'],
                    self.format_book_number(self.book_info['book_number'])
                )
            else:
                return self.book_info['short_title']
        else:
            return ""

    def format_book_number(self, book_number):
        if str(book_number)[-2:] == ".0":
            return int(book_number)
        else:
            return book_number

    def format_link(self):
        title = self.build_long_title()
        url = self.book_info["master_grlink"]
        author = self.book_info["first_author"]
        nth = self.nth + 1
        total = self.total
        if total != 1:
            prefix = f"\#{nth}/{total}: "
        else:
            prefix = ""
        score = self.score

        if score < config['matching']['min_ratio']:
            return f"{prefix}**Search Failed** ^(&#40;Found [{title}]({url}) with bad matching score of {score}% ⚠️&#41;)"
        else:
            return f"{prefix}**[{title}]({url}) by {author}** ^(&#40;Matching {score}% ☑️&#41;)"

    def format_description(self):
        description = self.book_info["summary"]
        if description is None:
            return "\n\n> **Summary:** ?"
        return '\n\n'+textwrap.shorten(
            "> **Summary:** " + description.replace('&gt;', ">"), width=500, placeholder=" (...)")

    def format_book_footer(self):
        n_sugg = self.book_info['n_bot']
        pages = self.book_info["pages"] or "?"
        year = self.book_info["year"] or "?"
        s = "s" if n_sugg > 1 else ""
        return f"\n^({pages} pages | Published: {year} | Suggested {n_sugg} time{s})"

    def format_tags(self):
        if self.book_info['tags']:
            tags = [str.capitalize(tag) for tag in ast.literal_eval(self.book_info['tags']) if len(tag) < 30]
            tags = list(itertools.takewhile(lambda x: ')' not in x, tags)) # Cleaning because DB has some corrupted data
            return "\n> **Themes**: " + ", ".join(tags)
        else:
            return ""

    def format_recos(self):
        recos = bq.get_top_2_books(self.book_info['master_grlink'])
        if len(recos) >= 2:
            title1 = recos.iloc[0, recos.columns.get_loc('title')]
            url1 = recos.iloc[0, recos.columns.get_loc('grlink')]
            author1 = recos.iloc[0, recos.columns.get_loc('author')]
            title2 = recos.iloc[1, recos.columns.get_loc('title')]
            url2 = recos.iloc[1, recos.columns.get_loc('grlink')]
            author2 = recos.iloc[1, recos.columns.get_loc('author')]
            return f"\n> **Top 2 recommended-along**: [{title1}]({url1}) by {author1}, [{title2}]({url2}) by {author2}"
        else:
            return ""

    def format_all(self):
        if self.score < config['matching']['min_ratio']:
            return '\n' + self.format_link() + '\n'
        else:
            parts = [
                self.format_link(),
                self.format_book_footer(),
                self.format_description(),
                self.format_tags(),
                self.format_recos()
            ]
            return '\n'.join([part for part in parts if len(part) > 1])
