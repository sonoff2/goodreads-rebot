from grbot.configurator import config
from grbot.utils import alphanumeric
from grbot import bq

from collections import defaultdict
from itertools import chain
from rapidfuzz import process, fuzz
from copy import deepcopy
import logging


class Query:
    def __init__(self, query):
        self.q = query
        self.clean_q = alphanumeric(str.lower(query))
        self.has_by = ' by ' in self.clean_q

class Book:
    def __init__(self, book_id, title=None, author=None, is_series=False):
        self.id = book_id
        self.title = title
        self.clean_title = alphanumeric(str.lower(self.title))
        self.author = author
        self.clean_author = alphanumeric(str.lower(self.author))
        self.is_series = is_series
        self.info = {}

    def get_info(self):
        if self.is_series:
            self.info = bq.get_info(
                bq.book_id_from_series_id([self.id])
            )
        else:
            self.info = bq.get_info([self.id])
        return self


class Match:
    def __init__(self, searched_string, score=None, is_serie=None, book=None):
        self.searched_string = searched_string
        self.score = score
        self.is_serie = is_serie
        self.matched_book = book

    def validate(self, threshold=config['matching']['min_ratio']):
        try:
            return self.score > threshold
        except:
            return False

class Matcher:

    def __init__(self, query, config=config):

        # Inputs
        self.query = Query(query)

        # Matching data:
        self.books_titles = self.init_book_list()
        self.series_titles = self.init_series_list()

        self.books_by_author = self.init_book_list_by_author()
        self.series_by_author = self.init_series_list_by_author()

        self.min_ratio = config['matching']['min_ratio']
        self.author_min_ratio = config['matching']['author_min_ratio']

        # Result
        self.possible_matches = [] # List of Match
        self.result = None         # Book

    def init_book_list(self):
        book_db = bq.get_book_titles()
        return [Book(book_id, title, author) for book_id, title, author in zip(
            book_db['book_id'], book_db['short_title'], book_db['author']
        )]

    def init_series_list(self):
        series_db = bq.get_series_titles()
        return [Book(series_id, title, author, is_series=True) for series_id, title, author in zip(
            series_db['series_id'], series_db['series_title'], series_db['author']
        )]

    def init_book_list_by_author(self):
        dic = defaultdict(list)
        for book in self.books_titles:
            last_name = book.clean_author.split(' ')[-1]
            dic[last_name].append(book)
        return dic

    def init_series_list_by_author(self):
        dic = defaultdict(list)
        for series in self.series_titles:
            last_name = series.clean_author.split(' ')[-1]
            dic[last_name].append(series)
        return dic

    def process(self):

        self.possible_matches = []
        if len(self.query.q) > 150:
            return self

        if self.query.has_by: # Maybe the user provided the author:
            self.possible_matches += self.match_process_filtered_on_author(search_series=False) \
                                     + self.match_process_filtered_on_author(search_series=True)
            if self.has_a_valid_match():
                return self.pick_best_match(config['matching']['draw_settle_key'])
            else:
                self.possible_matches = []

        self.possible_matches += self.match_process(title=self.query.q, search_series=False) \
                                 + self.match_process(title=self.query.q, search_series=True)

        best_match = self.pick_best_match(draw_settle_key='sort_n')

        return best_match

    def has_a_valid_match(self, matches_list=None):
        max_score = max(match.score for match in (self.possible_matches if matches_list is None else matches_list))
        if max_score >= self.min_ratio:
            return True
        else:
            return False

    def pick_best_match(self, draw_settle_key):
        self.enrich_possible_matches()
        max_score = max(match.score for match in self.possible_matches)
        if max_score < self.min_ratio:
            return None
        else:
            best_matches = [match for match in self.possible_matches if match.score >= max_score]
            if len(best_matches) == 1:
                return best_matches[0]
            else:
                return sorted(best_matches, key=lambda match: match.book.info[draw_settle_key], reverse=True)[0]

    def enrich_possible_matches(self):
        info_dic = bq.get_info([match.book.id for match in self.possible_matches])
        for match in self.possible_matches:
            match.book.info = info_dic[match.book.id]
        return self

    def match_process_filtered_on_author(self, search_series=False):
        title, author = self.query.clean_q.rsplit(' by ', 1)
        last_name = author.split(' ')[-1]
        search_dic = self.series_by_author if search_series else self.books_by_author
        closest_authors = [fuzz_result[0] for fuzz_result in
            process.extract(last_name, list(search_dic.keys()), scorer=fuzz.ratio, limit=5)
            if fuzz_result[1] > self.author_min_ratio
        ]
        books_by_closest_authors = list(chain(*[search_dic[key] for key in closest_authors]))
        return self.match_process(
            title,
            search_list=books_by_closest_authors,
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
                title, search_list[:int(len(search_list)*0.1)], search_series, k=k, func=func)
            if max(match.score for match in possible_matches_on_top) >= config['matching']['min_ratio']:
                return possible_matches_on_top

        # Step 2:
        return self._match_fuzz(
            searched_string=title,
            search_list=search_list,
            is_serie=search_series,
            k=k, func=func
        ) + self.match_start_of_titles(
            title=title,
            search_list=search_list,
            search_series=search_series)

    def match_start_of_titles(self, title, search_list=None, search_series=False):
        if search_list is None:
            search_list = self.series_titles if search_series else self.books_titles
        new_search_list = deepcopy(search_list)
        query_len = len(title)
        for book in new_search_list:
            book.clean_title = book.clean_title[0:query_len]
        return self._match_fuzz(title, new_search_list, is_serie=search_series, func='full', lateralize=False)

    def _match_fuzz(self, searched_string, search_list, is_serie, k=3, func="partial", lateralize=True):
        scorer = fuzz.partial_ratio
        if func != "partial":
            scorer = fuzz.ratio
        if lateralize:
            search_list = [ss.ljust(len(s), '#') for ss in search_list]
        logging.info(f"Matching {searched_string} with a list of len {len(search_list)}")
        return [Match(searched_string, result[1], is_serie, search_list[result[2]])
                for result in process.extract(searched_string,
                                              [book.clean_title for book in search_list],
                                              scorer=scorer,
                                              limit=k)]


