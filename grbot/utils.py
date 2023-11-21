import logging
import re
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

