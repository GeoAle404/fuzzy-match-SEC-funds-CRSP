'''
File: c1_fuzz_scorer.py
Project: Extract Location

File Created: Thursday, 12th November 2020 9:12:34 am

Author: Georgij Alekseev (geo.ale104@gmail.com)
-----
Last Modified: Wednesday, 12th January 2022 9:54:04 pm
-----
Description: Fuzzy match scorer class
'''
# =================================================================================================
# PACKAGES
# =================================================================================================
from fuzzywuzzy.StringMatcher import StringMatcher as SequenceMatcher
# slower alternative: from difflib import SequenceMatcher


# =================================================================================================
# FUZZYSCORER
# =================================================================================================
class FuzzyScorer:
    """
    The FuzzyScorer class is based on the Ratio measure of the SequenceMatcher.
    However, this class exands by allowing different penalties depending on the un-matched characters.
    In the current functionality, we can attach different weight to un-matched digits.
    """

    def __init__(self, to_match, choice=None, digit_multiplier=1, sorted_token=False):
        """
        Initialize SequenceMatcher and set variables for later use
        Arguments:
            choice (string) - will be compared with other string; it is faster to change this string
            to_match (string) - will be compared with other string
            digit_multiplier (numeric) - penalty multiplier for non-matched digits
        """
        if sorted_token:
            self.seq1 = self.sort_and_token(choice)
            self.seq2 = self.sort_and_token(to_match)
        else:
            self.seq1 = choice
            self.seq2 = to_match

        self.seqmat = SequenceMatcher(isjunk=None, seq1=self.seq1, seq2=self.seq2)

        self.digit_multiplier = digit_multiplier
        self.sorted_token = sorted_token

    def set_choice(self, choice):
        """
        It is faster to change the first string than the second, see SequenceMatcher documentation.
        Arguments:
            choice (string) - will be compared with other string
        """
        if self.sorted_token:
            self.seq1 = self.sort_and_token(choice)
        else:
            self.seq1 = choice
        self.seqmat.set_seq1(self.seq1)

    def compute_score(self):
        """
        This function computes the Ratio Score with penalty multiplier for digits.
        """
        # nothing to do if one of the strings is zero length
        len1, len2 = len(self.seq1), len(self.seq2)
        if len1 == 0 and len2 == 0:
            return 100
        if len1 == 0 or len2 == 0:
            return 0

        if self.seqmat is None:
            self.seqmat = SequenceMatcher(isjunk=None, seq1=self.seq1, seq2=self.seq2)

        # first identify the matching blocks for each string
        matching_blocks = self.seqmat.get_matching_blocks()

        # first identify the position of the un-matched characters for both strings
        # diff holds the positions of the un-matched characters
        diff_a, diff_b = [], []
        cur_pos_a = cur_pos_b = 0
        for start_a, start_b, length in matching_blocks:

            diff_a += list(range(cur_pos_a, start_a))
            diff_b += list(range(cur_pos_b, start_b))

            cur_pos_a = start_a + length
            cur_pos_b = start_b + length

        # compute the penalty score
        # this score is based on the number of un-matched characters for both strings,
        # where a multiplier is applied to digits
        penalty = 0

        for i in diff_a:
            if self.seq1[i].isdigit():
                penalty += self.digit_multiplier
            else:
                penalty += 1

        for i in diff_b:
            if self.seq2[i].isdigit():
                penalty += self.digit_multiplier
            else:
                penalty += 1

        # now compute the number of matched characters
        # match length is last entry in each tuple of matching_blocks() output
        matches = sum(triple[-1] for triple in matching_blocks)

        # final score is the ratio of matches and (matches+penalty)
        # between 0 and 100
        return int(100*(matches/(matches+penalty)))

    def compute_sorted_token_score(self):
        """
        Computes the score after tokenizing and sorting the words in the strings.
        E.g., "High Risk" and "Risk High" would get a score of 100.
        """
        # tokenize and sort, but backup the original strings
        seq1, seq2 = self.seq1, self.seq2
        self.seq1 = ' '.join(sorted(seq1.split()))
        self.seq2 = ' '.join(sorted(seq2.split()))
        self.seqmat = SequenceMatcher(isjunk=None, seq1=self.seq1, seq2=self.seq2)

        # compute score, retrieve backup and return
        score = self.compute_score()
        self.seq1, self.seq2 = seq1, seq2
        self.seqmat = None
        return score

    @staticmethod
    def sort_and_token(seq):
        """
        Sorts words in string alphabetically.
        Ensures that, e.g., "High Risk" and "Risk High" would get a score of 100.
        """
        return ' '.join(sorted(seq.split())) if seq is not None else ''

    def real_quick_ratio(self):
        """
        Returns an upper bound on the score very quickly.
        There can't be more matches than the number of elements in the shorter sequence
        """
        len1, len2 = len(self.seq1), len(self.seq2)

        if len1 == 0 and len2 == 0:
            return 100

        return int(100 * 2.0 * min(len1, len2) / (len1 + len2))
