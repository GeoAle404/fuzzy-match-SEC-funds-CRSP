'''
File: f2_fuzzy_match.py
Project: Extract Location

File Created: Saturday, 7th November 2020 8:23:32 am

Author: Georgij Alekseev (geo.ale104@gmail.com)
-----
Last Modified: Wednesday, 12th January 2022 9:53:45 pm
-----
Description: This code provides functions for the fuzzy matching.
'''
# ======================================================================================================================
# PACKAGES
# ======================================================================================================================
import re
import pandas as pd
import numpy as np


# ======================================================================================================================
# FUNCTIONS
# ======================================================================================================================
def preproc_string(strings, trim_words=None, regex_sub=None, rem_reg_from_fund=False):
    """
    This function preprocesses a string or a list of strings for fuzzy matching.
    Args:
        strings - string or list of strings, must be of the form 'REGISTRANT: FUND'
        trim_words - list of words that will be removed
        regex_sub (regex, sub) - pre-compiled regex substitution pairs
        rem_reg_from_word (boolean) - if true, removes all registrant words from fund name
    """
    # check eligibility of strings
    if len(strings) == 0:
        return None
    # convert to list if not a list yet
    strings = strings if isinstance(strings, (list, tuple, pd.Series, np.ndarray)) else [strings]

    # to lower case
    strings = [s.lower() for s in strings]
    # split registrant and fund name
    splits = [s.split(sep=':', maxsplit=1) for s in strings]
    st_r, st_f = zip(*[s if len(s) == 2 else s+[''] for s in splits])
    # replace trim words with space (e.g., 'and', 'corp', and 'inc')
    if trim_words is not None:
        for trim_word in trim_words:
            st_r = [re.sub(rf'\b{trim_word}\b', ' ', e) for e in st_r]
            st_f = [re.sub(rf'\b{trim_word}\b', ' ', e) for e in st_f]
    # apply additional regex substitutions if specified
    if regex_sub is not None:
        for reg, sub in regex_sub:
            st_r = [reg.sub(sub, e) for e in st_r]
            st_f = [reg.sub(sub, e) for e in st_f]
    # keep only letters and digits; replace everything else with '' (empty)
    st_r = [re.sub(r'[^a-zA-Z\d\s]+', '', e) for e in st_r]
    st_f = [re.sub(r'[^a-zA-Z\d\s]+', '', e) for e in st_f]
    # remove extra spaces and '&'
    st_r = [re.sub(r'\s*[& ]\s*', ' ', e) for e in st_r]
    st_f = [re.sub(r'\s*[& ]\s*', ' ', e) for e in st_f]
    # remove all registrants words also appearing in the fund
    if rem_reg_from_fund:
        st_f = [[word for word in f.split() if word not in r.split()] for r, f in zip(st_r, st_f)]
        st_f = [' '.join(f) for f in st_f]
    # remove trailing and leading spaces
    st_r = [e.strip() for e in st_r]
    st_f = [e.strip() for e in st_f]

    # convert to array for speed gains
    st_r = np.array(st_r)
    st_f = np.array(st_f)
    # return string again if single element
    st_r = st_r[0] if len(st_r) == 1 else st_r
    st_f = st_f[0] if len(st_f) == 1 else st_f
    return st_r, st_f
