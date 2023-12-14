import logging
import numpy as np
import re
import pickle

from rapidfuzz import process, fuzz


def is_submission(reddit_post):
    if hasattr(reddit_post, 'selftext'):
        return True
    else:
        return False


# MATCHING TITLES UTILS:

def replace_nan(var, replacement="?"):
    if str(var) in ['nan', 'None', '<NA>', 'NaN', 'NA']:
        return replacement
    else:
        return var

def extract_braces(comment_body):
    sub_parts = re.findall(r'\{\{([^}]*)\}\}', comment_body)
    return sub_parts

def partial_ratio(s1, s2):
    return fuzz.partial_ratio(str.lower(s1), str.lower(s2))

def alphanumeric(s):
    return re.sub(r'[^a-zA-Z0-9\s]', '', s, flags=re.UNICODE)

def extract_last_name(s):
    try:
        return next(word for word in reversed(s.split()) if len(word) >= 2)
    except:
        return s

def remove_zeros(x):
    if re.fullmatch(r'\d+\.0', x):
        return re.sub(r'\.0$', '', x)
    return x

def clean_start(s, words_to_exclude):
    for word in words_to_exclude:
        if s.startswith(word):
            return s[len(word):]
    return s

def load_pickle(path):
    with open(path, 'rb') as handle:
        r = pickle.load(handle)
    return r

def humanize_number(value, fraction_point=1):
    try:
        value = int(value)
    except:
        return np.nan
    powers = [10 ** x for x in (12, 9, 6, 3, 0)]
    human_powers = ('T', 'B', 'm', 'k', '')
    is_negative = False
    if not isinstance(value, float):
        value = float(value)
    if value < 0:
        is_negative = True
        value = abs(value)
    for i, p in enumerate(powers):
        if value >= p:
            return_value = str(round(value / (p / (10.0 ** fraction_point))) / (10 ** fraction_point)) + human_powers[i]
            break
    if is_negative:
        return_value = "-" + return_value

    return return_value

def replace_if_falsy(x, replace_value, func=None):
    if str(x) in ['[]', 'None', 'nan', '', '<NA>']:
        return replace_value
    else:
        return func(x) if func is not None else x

def comment_triggers(comment):
    txt = comment.selftext if is_submission(comment) else comment.body
    txt = txt.replace("((", "{{").replace("))", "}}")
    if ("{{" in txt) and ("}}" in txt):
        return True
    else:
        return False