from grbot import bq
from grbot.configurator import config

import logging
import itertools
import textwrap
import urllib.parse

class Formatter:
    def __init__(self, best_match, nth, total, book_requested):
        self.title = best_match.book.title
        self.nth = nth
        self.total = total
        self.score = int(best_match.raw_score)
        self.is_series = best_match.is_serie
        self.book_requested = book_requested
        self.book_info = best_match.book.info
        logging.info(f"Created Formatter : {self.__dict__}")

    def build_long_title(self):
        if hasattr(self, "book_info"):
            if self.book_info["series_title"] is not None:
                return "{} ({} #{})".format(
                    self.book_info['book_title'],
                    self.book_info['series_title'],
                    self.format_book_number(self.book_info['book_number'])
                )
            else:
                return self.book_info['book_title']
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
        author = self.book_info["author"]
        nth = self.nth + 1
        total = self.total
        if total != 1:
            prefix = f"\n\#{nth}/{total}: "
        else:
            prefix = ""
        score = self.score

        if score < config['matching']['min_ratio']:
            display_result = score > 70
            goodreads_url = "https://www.goodreads.com/search?q="+urllib.parse.quote_plus(
                self.book_requested.replace(' by', ''))
            return (
                f"""{prefix}⚠ Could not *exactly* find "*{self.book_requested}*" """
                + f"""but found [{title}]({url}) ^(&#40;with matching score of {score}%  &#41;)"""*display_result
                + f", see [related Goodreads search results]({goodreads_url}) instead."
            )
        else:
            return f"""{prefix}**[{title}]({url}) by {author}** ^(&#40;Matching {score}% ☑️&#41;)"""

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
        s = "s" if (n_sugg or 0) > 1 else ""
        n_sugg = n_sugg or "?"
        return f"\n^({pages} pages | Published: {year} | Suggested {n_sugg} time{s})"

    def format_tags(self):
        if len(self.book_info['tags']) > 0:
            tags = [str.capitalize(tag) for tag in self.book_info['tags'] if len(tag) < 30]
            tags = list(itertools.takewhile(lambda x: ')' not in x, tags))[0:7] # Cleaning because DB has some corrupted data
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

    def format_links(self):
        if self.nth + 1 == self.total:
            return """\n^( [Provide Feedback](https://www.reddit.com/user/goodreads-rebot) | [Source Code](https://github.com/sonoff2/goodreads-rebot) | ["The Bot is Back!?"](https://www.reddit.com/r/suggestmeabook/comments/16qe09p/meta_post_hello_again_humans/))"""
        else:
            return ""

    def default_failed_text(self):
        return "\n^(*Possible reasons for mismatch: either too recent &#40;2023&#41;, mispelled &#40;check Goodreads&#41; or too niche. Please note we are working hard on a major update for beginning of Dec 2023.*)\n"

    def format_all(self):
        if self.score < config['matching']['min_ratio']:
            parts = [
                self.format_link(),
                self.default_failed_text(),
                self.format_links()
            ]
        else:
            parts = [
                self.format_link(),
                self.format_book_footer(),
                self.format_description(),
                self.format_tags(),
                self.format_recos(),
                self.format_links()
            ]
        return '\n'.join([part for part in parts if len(part) > 1])
