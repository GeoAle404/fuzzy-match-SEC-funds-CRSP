'''
File: c2_fuzz_matcher.py
Project: Extract Location

File Created: Thursday, 12th November 2020 10:27:55 am

Author: Georgij Alekseev (geo.ale104@gmail.com)
-----
Last Modified: Wednesday, 12th January 2022 9:53:33 pm
-----
Description: Custom fuzzy matcher.
'''
# =================================================================================================
# PACKAGES
# =================================================================================================
from collections import namedtuple
from math import comb
import re
import time
import pandas as pd
import numpy as np
from pathos.multiprocessing import ProcessingPool as Pool

from f1_preproc_strings import preproc_string
from c2_fuzz_scorer import FuzzyScorer


# =================================================================================================
# FUZZYMATCHER
# =================================================================================================
class FuzzyFundMatcher:
    """
    This classes fuzzy matches a list of fund names
    with the best candidate(s) of a different list of fund names.
    """

    def __init__(self, to_matches, choices, match_pos=None):
        """
        Initialize FuzzyFundMatcher and set variables for later use
        Arguments:
            to_matches (string | list of strings) - strings that need to be matched with the choices
            choices (string | list of strings) - choices that can be matched with each to_matches
            match_pos (list) - identifiers of choices, e.g., fund number or row index
        """
        self.to_matches = to_matches
        self.choices = choices

        self.to_matches_r = self.to_matches_f = None
        self.choices_r = self.choices_f = None

        # keep track of match positions, they will be returned to assist in identification
        # e.g., to get fund number of match or other information
        # if match position is specified, use it
        # if object is series, extract it
        # else, generate it
        if match_pos is not None:
            self.match_pos = match_pos
        elif isinstance(choices, pd.Series):
            self.match_pos = np.array(choices.index)
        else:
            self.match_pos = np.array(range(0, len(self.choices)))

    def preproc(self, trim_words=None, regex_sub=None, rem_reg_from_fund=False, combine_reg_fund=False):
        """
        Preprocess the strings to assist in matching.
        Arguments:
            trim_words (list of strings) - words that will be removed, also known as stop words
            regex_sub (list of tuples: (regex expression, substitution)) - regex substitutions to apply
        """
        # pre-compile the regular expressions
        regex_sub = [(re.compile(regex), sub) for regex, sub in regex_sub]

        # combine registrant and fund name by removing the separator
        if combine_reg_fund:
            self.to_matches = [e.replace(':', ' ') for e in self.to_matches]
            self.choices = [e.replace(':', ' ') for e in self.choices]

        # apply basic preprocessing
        try:
            start = time.time()

            self.to_matches_r, self.to_matches_f = preproc_string(self.to_matches,
                                                                  trim_words=trim_words,
                                                                  regex_sub=regex_sub,
                                                                  rem_reg_from_fund=rem_reg_from_fund)

            self.choices_r, self.choices_f = preproc_string(self.choices,
                                                            trim_words=trim_words,
                                                            regex_sub=regex_sub,
                                                            rem_reg_from_fund=rem_reg_from_fund)

            print(f"Applying regular expressions took {int(time.time()-start)} seconds.")
        except TypeError:
            return

    def find_best(self, top_n=3, valid_threshold=0, digit_multiplier=1, multi_processing=False,
                  quick_comparison=False, order_irrelevance=False):
        """
        Find the best matching choices for each to_match
        Arguments:
            top_n (integer) - number of top results to be included in output
            valid_threshold (numeric, in 0-100) - threshold for registrant and fund stage
            digit_multiplier (numeric, >0) - multiplier for un-matched digit penalty
        """
        # prepare function and inputs for multiprocessing pool or single-core list comprehension
        find_func = FuzzyFundMatcher.wrapper_find_cur_best(self.choices_r, self.choices_f,
                                                           self.match_pos, top_n=top_n,
                                                           valid_threshold=valid_threshold,
                                                           digit_multiplier=digit_multiplier,
                                                           quick_comparison=quick_comparison,
                                                           order_irrelevance=order_irrelevance)

        RegFund = namedtuple('Registrant_Fund', 'registrant fund')
        reg_funds = [RegFund(reg, fund) for reg, fund in zip(self.to_matches_r, self.to_matches_f)]

        # compute
        if multi_processing:
            # compute and terminate pool for next iteration
            pool = Pool()
            matches = pool.map(find_func, reg_funds)
            pool.terminate()
            pool.restart()
        else:
            # single-core list comprehension
            matches = [find_func(rf) for rf in reg_funds]

        # finish output
        output = []
        Match = namedtuple('Match', 'to_match matches')
        for mat, registrant, fund in zip(matches, self.to_matches_r, self.to_matches_f):
            if mat:
                # output format: [[name, (m_name1, m_score1, m_pos1),
                #                  (m_name2, m_score2, m_pos2)], ...]
                output.append(Match(f'{registrant} : {fund}', mat))
            else:
                output += [None]
        return output

    # staticmethod is required for efficient multiprocessing execution
    @staticmethod
    def wrapper_find_cur_best(choices_r, choices_f, match_pos, top_n, valid_threshold, digit_multiplier,
                              quick_comparison=False, order_irrelevance=False):
        """
        Wrapper for efficient multiprocessing execution of find_cur_best
        """
        return lambda regfund: FuzzyFundMatcher.find_cur_best(regfund.registrant, regfund.fund, choices_r, choices_f,
                                                              match_pos, top_n, valid_threshold, digit_multiplier,
                                                              quick_comparison, order_irrelevance)

    # staticmethod is required for efficient multiprocessing execution
    @staticmethod
    def find_cur_best(registrant, fund, choices_r, choices_f, match_pos,
                      top_n=3, valid_threshold=0, digit_multiplier=1,
                      quick_comparison=False, order_irrelevance=False):
        """
        Find the best matching choice for the current to_match
        """
        # rcr (remaining choices registrants), rcf (remaining choices funds)
        # rrsc (remaining registrant scores), rpos (remaining positional identifiers)
        # =========================================================================================
        # QUICK COMPARISON
        # =========================================================================================
        # if quick_comparison is turned on, we first check the
        # maximum possible score (an upper bound) very fast
        # if the upper bound is too low, then discard immediately
        if quick_comparison:
            try:
                # first do check for registrant part
                # intialize fuzzy scorer
                fus = FuzzyScorer(to_match=registrant,
                                  digit_multiplier=digit_multiplier)
                valid = []
                for choice in choices_r:
                    fus.set_choice(choice)
                    valid.append(fus.real_quick_ratio() >= valid_threshold)
                valid = np.array(valid)

                # only keep remaining candidates
                rcr, rcf = choices_r[valid], choices_f[valid]
                rpos = match_pos[valid]

                # repeat for fund part
                fus = FuzzyScorer(to_match=fund,
                                  digit_multiplier=digit_multiplier)
                valid = []
                for choice in rcf:
                    fus.set_choice(choice)
                    valid.append(fus.real_quick_ratio() >= valid_threshold)
                valid = np.array(valid)

                # only keep remaining candidates
                rcr, rcf = rcr[valid], rcf[valid]
                rpos = rpos[valid]

            except TypeError:
                return None
            except IndexError:
                return None

        # =========================================================================================
        # REGISTRANT MATCHING
        # =========================================================================================
        # first match registrants and only keep valid candidates
        try:
            # intialize fuzzy scorer
            fus = FuzzyScorer(to_match=registrant,
                              digit_multiplier=digit_multiplier)

            # compute score for every combination
            rrsc = []
            for choice in rcr:
                fus.set_choice(choice)
                rrsc.append(fus.compute_score())
            rrsc = np.array(rrsc)

            # re-do with tokenized matching
            if order_irrelevance:
                half_valid_threshold = int(valid_threshold/3) # this is on purpose, half is not enough
                rrsc_token = []
                fus = FuzzyScorer(to_match=registrant,
                                  digit_multiplier=digit_multiplier,
                                  sorted_token=True)
                for choice, cur_rrsc in zip(rcr, rrsc):
                    # no need to try if score is 100 or less than half of valid_threshold
                    if cur_rrsc == 100:
                        rrsc_token.append(100)
                    elif cur_rrsc < half_valid_threshold:
                        rrsc_token.append(0)
                    else:
                        fus.set_choice(choice)
                        rrsc_token.append(fus.compute_score())

                rrsc_token = np.array(rrsc_token)

                # only keep the best of both and keep track of improvements
                improvement = rrsc_token > rrsc
                rrsc = np.maximum(rrsc, rrsc_token)

            # flag scores that are sufficiently high
            valid = np.array([e >= valid_threshold for e in rrsc])

            # check whether at least one match, otherwise abort
            if ~valid.any():
                return None

            # only keep remaining candidates
            rcr, rcf = rcr[valid], rcf[valid]
            rrsc = rrsc[valid]
            rpos = rpos[valid]
            if order_irrelevance:
                improvement = improvement[valid]
        except TypeError:
            return None

        # =========================================================================================
        # FUND MATCHING
        # =========================================================================================
        # now find the best funds within the remaining candidates
        try:
            # intialize fuzzy scorer
            fus = FuzzyScorer(to_match=fund,
                              digit_multiplier=digit_multiplier)

            # compute score for every combination
            rfsc = []
            for choice in rcf:
                fus.set_choice(choice)
                rfsc.append(fus.compute_score())
            rfsc = np.array(rfsc)

            # re-do with tokenized matching if no perfect match found yet
            if order_irrelevance:
                rfsc_token = []
                fus = FuzzyScorer(to_match=fund,
                                  digit_multiplier=digit_multiplier,
                                  sorted_token=True)
                for choice, cur_rfsc in zip(rcf, rfsc):
                    # no need to try if score is zero
                    if cur_rfsc == 100:
                        rfsc_token.append(100)
                    elif cur_rfsc < half_valid_threshold:
                        rfsc_token.append(0)
                    else:
                        fus.set_choice(choice)
                        rfsc_token.append(fus.compute_score())
                rfsc_token = np.array(rfsc_token)

                # only keep the best of both and keep track of improvements
                improvement = improvement + (rfsc_token > rfsc)
                rfsc = np.maximum(rfsc, rfsc_token)

            # flag scores that are sufficiently high
            valid = np.array([e >= valid_threshold for e in rfsc])

            # check whether at least one match, otherwise abort
            if ~valid.any():
                return None

            # only keep remaining candidates
            rcr, rcf = rcr[valid], rcf[valid]
            rrsc, rfsc = rrsc[valid], rfsc[valid]
            rpos = rpos[valid]
            if order_irrelevance:
                improvement = improvement[valid]
        except TypeError:
            return None

        # =========================================================================================
        # FINALIZE
        # =========================================================================================
        # compute weighted score, round down, and convert to integer
        scores_weighted = np.sqrt(rrsc*rfsc)
        scores_weighted = np.floor(scores_weighted).astype(int)

        # compute position of top n according to weighted score
        # keep all matches if total number is less than top n
        if (top_n is not None) and (len(scores_weighted) > top_n):
            pos_top = np.argpartition(scores_weighted, -top_n)[-top_n:]
            # argpartition does not ensure sorted order of top n, do this now
            pos_top = pos_top[np.argsort(scores_weighted[pos_top])][::-1]
        else:
            # sorting still needed
            pos_top = np.argsort(scores_weighted)[::-1]
        # return top n
        rcr, rcf = rcr[pos_top], rcf[pos_top]
        picks = [f'{r} : {f}' for r, f in zip(rcr, rcf)]
        scores_weighted = scores_weighted[pos_top]
        rpos = rpos[pos_top]

        # flag when order irrelevance improved matching
        flag_m = [''] * len(pos_top)
        if order_irrelevance:
            improvement = improvement[pos_top]
            flag_m = ['OR' if imp else fl for fl, imp in zip(flag_m, improvement)]

        # output is in format: [(fname1, score1, index1, flag1),
        #                       (fname2, score2, index2, flag2), ...]
        return list(zip(*[picks, scores_weighted, rpos, flag_m]))
