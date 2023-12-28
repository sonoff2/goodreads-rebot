from grbot.configurator import config
from grbot.matching import Queries
from grbot.utils import humanize_number, replace_if_falsy

import itertools
import textwrap
import urllib.parse

class Reply:

    BY_PREFIX = (
        "🚨 Note to u/{author}: including the **author name** after a **\"by\"** keyword will help the bot find "
        "the good book! (simply like this *{{{{Call me by your name by Andre Aciman}}}})"
    )

    STD_SUFFIX = (
        """^("""
        # """"**NEW** 👉 Downvote to remove | """
        """[Feedback](https://www.reddit.com/user/goodreads-rebot) | """
        """[GitHub](https://github.com/sonoff2/goodreads-rebot) | """
        """["The Bot is Back!?"](https://www.reddit.com/r/suggestmeabook/comments/16qe09p/meta_post_hello_again_humans/) | """
        f"""v{config['version']}"""
        """)"""
    )

    def __init__(self, formatters, author):
        self.formatters = formatters
        self.author = author
        self.prefix = self.build_prefix()
        self.suffix = self.build_suffix()

    def build_prefix(self):
        if Queries([formatter.book_requested for formatter in self.formatters]).one_has_no_by():
            return Reply.BY_PREFIX.format(author = self.author)
        else:
            return ""

    def build_suffix(self):
        return Reply.STD_SUFFIX

    def text(self):
        prefix = (self.prefix + "\n\n---\n\n" if len(self.prefix) > 0 else "")
        recos = "\n\n---\n".join([formatter.format_all() for formatter in self.formatters])
        suffix = ("\n" + self.suffix if len(self.suffix) > 0 else "")
        return prefix + recos + suffix


class Formatter:
    def __init__(self, best_match, nth, total, book_requested, books_recommended_info):
        self.title = best_match.book.title
        self.nth = nth
        self.total = total
        self.score = int(best_match.raw_score)
        self.is_series = best_match.is_serie
        self.book_requested = book_requested
        self.book_info = best_match.book.info
        self.reco_separators = (', ', "") if (self.total > 1) else ("", "\n>  \- ")
        self.books_recommended_info = books_recommended_info # List of dict [{'short_title': .., 'author': ..}]

    def shorten_desc(self, description):
        if self.total > 1:
            l = 300
        else:
            l = 400
        return textwrap.shorten(
            "> **Summary:** " + description.replace('&gt;', ">"), width=l, placeholder=" (...)")

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
        else:
            return '\n\n'+self.shorten_desc(description)

    def format_book_footer(self):
        n_rev = replace_if_falsy(self.book_info['ratings_count'], "?", humanize_number)
        pages = replace_if_falsy(self.book_info["pages"], "?", int)
        year = replace_if_falsy(self.book_info["year"], "?", int)
        s = "s" if (n_rev and (n_rev not in ["0", "1"])) else ""
        return f"\n^({pages} pages | Published: {year} | {n_rev} Goodreads review{s})"

    def format_tags(self):
        tags = self.book_info['tag_list'] or []
        if len(tags) > 0:
            tags = [str.capitalize(tag) for tag in self.book_info['tag_list'] if len(tag) < 30]
            tags = list(itertools.takewhile(lambda x: ')' not in x, tags))[0:7] # Cleaning because DB has some corrupted data
            return "\n> **Themes**: " + ", ".join(tags)
        else:
            return ""

    def format_recos(self):
        if len(self.books_recommended_info) > 0:
            start = f"""\n> **Top {len(self.books_recommended_info)} recommended:**  """
            separator_1, separator_2 = self.reco_separators
            reco_list = separator_1.join([
                f"{separator_2}[{book_info['book_title']}]({book_info['master_grlink']}) by {book_info['author']}  """
                for book_info in self.books_recommended_info
            ])
            return start + reco_list
        else:
            return ""

    def default_failed_text(self):
        return "\n^(*Possible reasons for mismatch: either too recent &#40;2023&#41;, mispelled &#40;check Goodreads&#41; or too niche. Please note we are working hard on a major update for beginning of Dec 2023.*)\n"

    def format_all(self):
        if self.score < config['matching']['min_ratio']:
            parts = [
                self.format_link(),
                self.default_failed_text()
            ]
        else:
            parts = [
                self.format_link(),
                self.format_book_footer(),
                self.format_description(),
                self.format_tags(),
                self.format_recos()
            ]
        return '\n'.join([part for part in parts if len(part) > 1])
