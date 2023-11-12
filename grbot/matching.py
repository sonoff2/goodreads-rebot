from grbot.configurator import config
from grbot.utils import alphanumeric, extract_last_name, remove_zeros, clean_start
from grbot import bq

from collections import defaultdict
from rapidfuzz import process, fuzz
import logging
import numpy as np

START_WORDS_TO_EXCLUDE = ['the ', 'a ', 'an ']

class Query:

    def __init__(self, query):
        self.q = query
        self.clean_q = clean_start(
            s=alphanumeric(str.lower(query)).strip(),
            words_to_exclude=START_WORDS_TO_EXCLUDE
        )
        self.has_by = ' by ' in self.clean_q

class Book:
    def __init__(self, book_id, title=None, author=None, is_series=False):
        self.id = book_id

        self.title = title
        self.clean_title = clean_start(
            s=alphanumeric(str.lower(self.title)),
            words_to_exclude=START_WORDS_TO_EXCLUDE
        )
        self.lateralized_title = ""
        self.short_title = ""

        self.author = author
        self.clean_author = alphanumeric(str.lower(self.author))
        self.clean_author_last_name = extract_last_name(self.clean_author)

        self.is_series = is_series
        self.info = {}

    def lateralize_title(self, until):
        self.lateralized_title = self.clean_title.ljust(until, '#')
        return self

    def shorten_title(self, at):
        self.short_title = self.clean_title[0:at]
        return self

    def get_info(self):
        if self.is_series:
            self.info = bq.get_info(
                bq.book_id_from_series_id([self.id])
            )
        else:
            self.info = bq.get_info([self.id])
        return self


class Match:
    def __init__(self, fuzz_score=None, is_serie=None, book=None, title_was_shortened=None):
        self.is_serie = is_serie
        self.title_was_shortened = title_was_shortened
        self.book = book
        self.raw_score = fuzz_score
        self.score = self.bonus_malus()

    def is_valid(self, threshold=config['matching']['min_ratio']):
        try:
            return self.score > threshold
        except:
            return False

    def bonus_malus(self):
        score = self.raw_score
        if self.is_serie:
            score += 5
        if self.title_was_shortened:
            score -= 5
        return score


class Matcher:

    def __init__(self, config=config):

        self.query = None

        # Matching data:
        self.book_db = bq.download_book_db(local_path=config['bq']['local_path_dim_books'])
        self.series_db = bq.download_series_db(local_path=config['bq']['local_path_dim_series'])

        self.series_id_to_book_id = self.find_series_first_book()

        self.books_titles = self.init_book_list()
        self.series_titles = self.init_series_list()

        self.books_by_author = self.init_book_list_by_author(is_serie=False)
        self.series_by_author = self.init_book_list_by_author(is_serie=True)

        self.min_ratio = config['matching']['min_ratio']
        self.author_min_ratio = config['matching']['author_min_ratio']

        # Result
        self.possible_matches = [] # List of Match
        self.result = None         # Book

    def init_book_list(self):
        return [Book(book_id, title, author) for book_id, title, author in zip(
            self.book_db['book_id'], self.book_db['book_title'], self.book_db['author']
        )]

    def init_series_list(self):
        return [Book(series_id, title, author, is_series=True) for series_id, title, author in zip(
            self.series_db['series_id'], self.series_db['series_title'], self.series_db['author']
        )]

    def init_book_list_by_author(self, is_serie=False):
        dic = defaultdict(list)
        for book in (self.series_titles if is_serie else self.books_titles):
            dic[book.clean_author_last_name].append(book)
        return dic

    def find_series_first_book(self):
        self.book_db['book_number_category'] = np.where(
            self.book_db['book_number'].apply(lambda s: str(s).isdigit() and s != "0"), 1, np.where(
            self.book_db['book_number'].str.contains("."), 2, np.where(
            self.book_db['book_number'].str.contains("-"), 3, 4
        )))
        return self.book_db.set_index("series_id").sort_values(["book_number_category", "book_number"])\
                           .groupby("series_id").head(1)["book_id"].to_dict()

    def process_queries(self, queries):
        return [
            self.process_one_query(query) for query in queries  # 10 matches max par post to avoid flood
        ]

    def process_one_query(self, query):

        self.query = Query(query)

        self.possible_matches = []
        if len(self.query.clean_q) > 150:
            return self

        if self.query.has_by: # Maybe the user provided the author:
            self.possible_matches += self.match_process_filtered_on_author(search_series=False) \
                                     + self.match_process_filtered_on_author(search_series=True)
            if self.has_a_valid_match():
                return self.pick_best_match(config['matching']['draw_settle_key'])

        self.possible_matches += self.match_process(title=self.query.clean_q, search_series=False) \
                                 + self.match_process(title=self.query.clean_q, search_series=True)

        best_match = self.pick_best_match(draw_settle_key='sort_n')

        return best_match

    def has_a_valid_match(self, matches_list=None):
        return any([match.is_valid() for match in (self.possible_matches if matches_list is None else matches_list)])

    def pick_best_match(self, draw_settle_key):
        self.enrich_possible_matches()
        self.possible_matches = [sorted(self.possible_matches, key=lambda match: match.score, reverse=True)[0]]
        max_score = self.possible_matches[0].score
        if max_score < self.min_ratio:
            return self.possible_matches[0]
        else:
            best_matches = [match for match in self.possible_matches if match.score >= max_score]
            if len(best_matches) == 1:
                return best_matches[0]
            else:
                return sorted(best_matches, key=lambda match: match.book.info[draw_settle_key], reverse=True)[0]

    def enrich_possible_matches(self):
        for match in self.possible_matches:
            if match.is_serie:
                match.book.info = self.retrieve_info_from_book_db(id = self.series_id_to_book_id[match.book.id])
            else:
                match.book.info = self.retrieve_info_from_book_db(id=match.book.id)
        return self

    def retrieve_info_from_book_db(self, id):
        return self.book_db.set_index('book_id').loc[id].to_dict()

    def match_process_filtered_on_author(self, search_series=False):
        title, author = self.query.clean_q.rsplit(' by ', 1)
        last_name = author.split(' ')[-1]
        search_book_dic = self.series_by_author if search_series else self.books_by_author
        closest_authors = [fuzz_result[0] for fuzz_result in
            process.extract(
                last_name,
                list(search_book_dic.keys()),
                scorer=fuzz.ratio,
                limit=5)
            if fuzz_result[1] > self.author_min_ratio
        ]
        search_book_list_filtered = [
            book for last_name in closest_authors for book in search_book_dic[last_name]
        ]
        return self.match_process(
            title,
            search_list=search_book_list_filtered,
            search_series=search_series,
            k=4,
            func="full")

    def match_process(self, title, search_list=None, search_series=False, k=4, func="full"):
        """
        2 steps:
        1) search matches among Top 10% of search_list
        2) search matches among all list AND on start of titles only
        :param search_series:
        :param k:
        :param func:
        :return:
        """
        if search_list is None:
            search_list = self.series_titles if search_series else self.books_titles

        # If not in a "filtered on author" case, we look on top 10% of books and series
        if len(search_list) > 1000:
            possible_matches_on_top = self._match_fuzz(
                searched_string=title,
                search_book_list=search_list[:int(len(search_list)*0.1)],
                is_serie=search_series,
                k=k,
                func=func)
            if max(match.score for match in possible_matches_on_top) >= config['matching']['min_ratio']:
                return possible_matches_on_top

        # Step 2:
        results = self._match_fuzz(
            searched_string=title,
            search_book_list=search_list,
            is_serie=search_series,
            k=k, func=func
        )
        if self.has_a_valid_match(results):
            return results
        else:
            return self.match_start_of_titles(
            searched_string=title,
            search_list=search_list,
            search_series=search_series
        )

    def match_start_of_titles(self, searched_string, search_list=None, search_series=False):
        return self._match_fuzz(
            searched_string=searched_string,
            search_book_list=[book.shorten_title(at=len(searched_string)) for book in search_list],
            search_attribute = "short_title",
            is_serie=search_series,
            title_was_shortened = True,
            func='full',
            lateralize=False,
            k=5)

    def _match_fuzz(
            self,
            searched_string,
            search_book_list,
            is_serie,
            title_was_shortened=False,
            k=3,
            search_attribute='clean_title',
            func="partial",
            lateralize=True,
    ):
        scorer = fuzz.partial_ratio
        if func != "partial":
            scorer = fuzz.ratio
        if lateralize:
            search_book_list = [book.lateralize_title(until=len(searched_string)) for book in search_book_list]
            search_attribute = "lateralized_title"
        logging.info(f"Matching {searched_string} with a list of len {len(search_book_list)}")
        return [
            Match(
                fuzz_score=result[1],
                is_serie=is_serie,
                title_was_shortened=title_was_shortened,
                book=search_book_list[result[2]]
            )
            for result in process.extract(
                searched_string,
                [getattr(book, search_attribute) for book in search_book_list],
                scorer=scorer,
                limit=k)
            ]


