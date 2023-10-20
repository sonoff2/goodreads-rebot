import pandas as pd

def dim_books_to_dim_series(
    dim_db,
    series_title_col='series_title',
    book_number_col='book_number',
    author_col='author',
    link_col='link',
    count_col='n_reco'
):
    return dim_db.dropna(subset=[series_title_col])\
                 .sort_values([series_title_col, book_number_col])\
                 .groupby([series_title_col, author_col, link_col])\
                 [count_col].sum().reset_index()
