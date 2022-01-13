'''
File: s4_fuzzy_match.py
Project: Extract Location

File Created: Tuesday, 3rd November 2020 11:04:48 am

Author: Georgij Alekseev (geo.ale104@gmail.com)
-----
Last Modified: Wednesday, 12th January 2022 9:53:44 pm
-----
Description: This code fuzzy matches the fund names used by the SEC filings with CRSP data.
'''
# =================================================================================================
# PACKAGES
# =================================================================================================
import os                               # Browse directories
import csv                              # CSV documents
import time
import math
import pandas as pd                     # DataFrames
import numpy as np

# import custom files
from c1_fuzz_matcher import FuzzyFundMatcher
from s2_preproc_regex_list import REGEX_SUB


# =================================================================================================
# SETTINGS - DIRECTORIES
# =================================================================================================
# specify NSAR parsed filings directory
NSAR_DIR = "'D:/path/subpath"

# specify holdings directory
HOLDING_DIR = "'D:/path/subpath"

# specify output directory
OUTPUT_DIR = "'D:/path/subpath"
OUTPUT_NAME = 'file_name.csv'


# =================================================================================================
# SETTINGS - MATCHING
# =================================================================================================
# words that will be trimmed in preprocessing (e.g., 'and', 'corp', and 'inc')
TRIM_WORDS = ['and', 'corp', 'fund', 'funds', 'fd', 'fds', 'fu', 'f',
              'inc', 'trust', 'the', 'corporation', 'portfolio']

# regular sub expressions that will be applied with re.sub on string s
# e.g., REGEX_SUB = [(f'/.{2,3}/\s*$', '')] removes /MA/ at end of the line
# they are imported at the beginning of the file from s4_2_preproc_regex_list.py
# (this here is just a placeholder)
REGEX_SUB = REGEX_SUB

# remove words appearing in registrant from fund name
REM_REG_FROM_FUND = True

# specify output in long or wide
OUTPUT_FORMAT = 'LONG' # 'LONG' or 'WIDE'

# number of top matches to be saved, e.g., 3 would save the first, second, and third best match
# NUMBER_MATCHES = None defaults to all matches satisfying VALID_THRESHOLD if OUTPUT_FORMAT == 'LONG'
# NUMBER_MATCHES = None default to NUMBER_MATCHES = 1 if OUTPUT_FORMAT == 'WIDE'
NUMBER_MATCHES = None

# specify matching horizon in months around CRSP holding date
MATCHING_HORIZON = (-8, 3)

# multiplier on penalty for un-matched digits
DIGIT_MULTIPLIER = 3

# sub category score threshold for consideration
# (e.g., 50 would require a minimum score of 50 in both registrant and fund matching)
VALID_THRESHOLD = 95

# specify whether the algorithm should do an additional comparison for non-perfect matches
# where there is no penalty for incorrect ordering,
# e.g., "High Yield" and "Yield High" would be equivalent
# note that matches improved by order irrelevance will be flagged with 'OR'
ORDER_IRRELEVANCE = True

# determines whether the algorithm first checks very fast if VALID_THRESHOLD is feasible
QUICK_COMPARISON = True

# size of randomly selected subset for every quarter, for testing purposes
SUBSET_SIZE = None
SEED = 15 # 13, 14, 15

# toggle multi core processing on or off
MULTI_PROCESSING = True


# =================================================================================================
# MULTIPROCESSING
# =================================================================================================
if __name__ == '__main__':

    # =============================================================================================
    # LOAD AND PREPROCESS NSAR FILINGS
    # =============================================================================================
    # load rd (registrant data) and fd (fund data)
    rd = pd.read_csv(os.path.join(NSAR_DIR, "nsar_registrants.csv"))
    fd = pd.read_csv(os.path.join(NSAR_DIR, "nsar_funds.csv"))

    # keep only relevant info for fuzzy match
    rd = rd[["cik", "fdate", "rdate", "reg_name", "file_name"]]
    fd = fd[["cik", "fdate", "fund", "fund_id", "file_name"]]

    # drop full duplicates
    rd = rd[~rd.duplicated()]
    fd = fd[~fd.duplicated()]

    # merge registrant with fund name to create nd (names data)
    nd = rd.merge(fd, how='left', on="file_name", validate="1:m", suffixes=('', '_y'))

    # replace NaN with empty string
    nd = nd.replace(np.nan, '', regex=True)

    # drop incomplete data
    nd = nd[nd.fdate != '']

    # combine registrant and fund names
    nd['match_name'] = nd["reg_name"] + " : " + nd["fund"]

    # drop duplicates
    nd = nd[~nd.duplicated(subset=['rdate', 'match_name'])]

    # convert date format
    nd["fdate"] = pd.to_datetime(nd["fdate"], format="%Y%m%d")
    nd["rdate"] = pd.to_datetime(nd["rdate"], format="%Y%m%d")
    nd["fyear"], nd["ryear"] = nd["fdate"].dt.year, nd["rdate"].dt.year
    nd["fmonth"], nd["rmonth"] = nd["fdate"].dt.month, nd["rdate"].dt.month

    # year-month
    nd["ryearmonth"] = 100*nd["ryear"] + nd["rmonth"]
    nd["fyearmonth"] = 100*nd["fyear"] + nd["fmonth"]


    # =============================================================================================
    # LOAD AND PREPROCESS HOLDINGS DATA
    # =============================================================================================
    # load crsp (CRSP holdings)
    crsp = pd.read_stata(os.path.join(HOLDING_DIR, 'CRSP_fund_characteristics_quarterly.dta'))

    # keep only relevant columns
    crsp = crsp[['caldt', 'fund_name', 'crsp_fundno']]

    # drop incomplete data
    crsp = crsp[crsp.fund_name != ""]

    # remove shares class info in crsp fund names
    crsp['fund_name'] = [entry.rsplit(sep=";", maxsplit=1)[0] for entry in crsp['fund_name']]

    # convert date format
    crsp["caldt"] = pd.to_datetime(crsp["caldt"], format="%Y%m%d")
    crsp["year"] = crsp["caldt"].dt.year
    crsp["month"] = crsp["caldt"].dt.month

    # year-month
    crsp["yearmonth"] = 100*crsp["year"] + crsp["month"]


    # =============================================================================================
    # PREPARE OUTPUT FILES
    # =============================================================================================
    # create empty CSV file that will be filled, and prepare writer
    csv_matches = open(os.path.join(OUTPUT_DIR, OUTPUT_NAME), 'w', newline='')
    writer_matches = csv.writer(csv_matches)

    # prepare and write CSV header
    csv_header = ['crsp_fundno', 'crsp_yearmonth', 'crsp_name_orig', 'crsp_name_preproc']
    outputs = ['m_name_orig', 'm_name_preproc', 'm_score', 'm_flag', 'm_cik',
               'm_fdate', 'm_fund_id', 'm_file_name']

    if OUTPUT_FORMAT == 'WIDE':
        number_outputs = len(outputs)
        outputs = outputs*NUMBER_MATCHES
        indices = np.repeat(list(range(1, NUMBER_MATCHES+1)), number_outputs)
        csv_header += [f"{name}{i}" for i, name in zip(indices, outputs)]

    elif OUTPUT_FORMAT == 'LONG':
        csv_header += outputs

    writer_matches.writerow(csv_header)
    csv_matches.close()


    # =============================================================================================
    # FUZZY MATCH
    # =============================================================================================
    # keep track of progress
    COUNT = 0

    # loop through quarters
    years = crsp.year.unique()
    months = [e*3 for e in range(1, 5)]

    for year in years:

        for month in months:
            # stop time for current iteration
            start = time.time()

            # compute current matching horizon
            start_year = year + math.floor((month + MATCHING_HORIZON[0] - 1)/12)
            end_year = year + math.floor((month + MATCHING_HORIZON[1] - 1)/12)
            start_month = (month + MATCHING_HORIZON[0] - 1)%12 + 1
            end_month = (month + MATCHING_HORIZON[1] - 1)%12 + 1

            start_yearmonth = 100*start_year + start_month
            end_yearmonth = 100*end_year + end_month

            # restrict matching on filings within the matching horizon
            cur_nd = nd[nd.ryearmonth.between(start_yearmonth, end_yearmonth)]
            cur_crsp = crsp[(crsp.year == year) & (crsp.month == month)]

            # check whether there are observations left in both sets
            if len(cur_nd) == 0 or len(cur_crsp) == 0:
                break

            # subset randomly selected for testing purposes
            if SUBSET_SIZE is not None:
                if SEED is not None:
                    np.random.seed(SEED)
                random_choice = np.random.choice(a=cur_crsp.index, size=SUBSET_SIZE, replace=False)
                cur_crsp = cur_crsp.loc[random_choice]

            # fuzzy match
            # initialize
            fm = FuzzyFundMatcher(to_matches=cur_crsp['fund_name'], choices=cur_nd['match_name'])
            # string preprocessing
            fm.preproc(trim_words=TRIM_WORDS, regex_sub=REGEX_SUB,
                       rem_reg_from_fund=REM_REG_FROM_FUND)
            # adjust number_matches variable (None returns all matches satisfying NUMBER_MATCHES)
            number_matches = NUMBER_MATCHES if (NUMBER_MATCHES is not None) or (OUTPUT_FORMAT == 'LONG') else 1
            # do the matching
            matches = fm.find_best(top_n=NUMBER_MATCHES, valid_threshold=VALID_THRESHOLD,
                                   digit_multiplier=DIGIT_MULTIPLIER, multi_processing=MULTI_PROCESSING,
                                   quick_comparison=QUICK_COMPARISON, order_irrelevance=ORDER_IRRELEVANCE)

            # identify the funds that got matched
            matched = np.array([e is not None for e in matches])
            matches = [e for e in matches if e is not None]
            cur_crsp = cur_crsp[matched]

            # prepare columns for data frame conversion
            columns = ['crsp_name_preproc']
            mat_cols = ['m_name_preproc', 'm_score', 'm_pos', 'm_flag']

            if OUTPUT_FORMAT == 'WIDE':
                # flatten the list of lists (slist stands for sub-list)
                # original format:  [[to_match, (m_name1, m_score1, m_pos1, m_flag1),
                #                               (m_name2, m_score2, m_pos2, m_flag2)], ...]
                # resulting format: [[to_match, m_name1, m_score1,
                #                     m_pos1, m_name2, m_score2, m_pos2, ...], ...]
                matches = [[m.to_match]+[e for sl in m.matches for e in sl] for m in matches]

                # fill up top N matches with '' if less than N reported
                matches = [e + [None]*(1+NUMBER_MATCHES*len(mat_cols)-len(e)) for e in matches]

                indices = np.repeat(list(range(1, NUMBER_MATCHES+1)), len(mat_cols))
                columns += [e1+str(e2) for e1, e2 in zip(mat_cols*NUMBER_MATCHES, indices)]

            elif OUTPUT_FORMAT == 'LONG':
                # resulting format: [[to_match, m_name1, m_score1, ...],
                #                    [to_match, m_name2, m_score2, ...], ...]
                # also add crsp fundno and crsp yearmonth
                tmp = []
                for m, fundno, yearmonth, fundname in zip(matches, cur_crsp['crsp_fundno'],
                                                          cur_crsp['yearmonth'], cur_crsp['fund_name']):
                    for sl in m.matches:
                        tmp.append([m.to_match] + list(sl) + [fundno, yearmonth, fundname])
                matches = tmp
                columns += mat_cols + ['crsp_fundno', 'crsp_yearmonth', 'crsp_name_orig']

            # convert to dataframe
            matches = pd.DataFrame(matches, columns=columns)

            if OUTPUT_FORMAT == 'WIDE':
                # use positional identifiers to retrieve cik, fdate and original names again again
                for i in range(1, NUMBER_MATCHES+1):
                    # first make sure m_pos column is numeric
                    # (it could be non-numeric when it's empty)
                    matches[f'm_pos{i}'] = pd.to_numeric(matches[f'm_pos{i}'])
                    # now merge on m_pos column
                    matches = matches.merge(cur_nd[['cik', 'fdate', 'match_name']], how='left',
                                            left_on=f'm_pos{i}', right_index=True, validate="m:1")
                    # apply suffix to merged info (cik, fdate, match_name, m_name_orig)
                    rename_dict = {'cik':f'm_cik{i}', 'fdate':f'm_fdate{i}', 'match_name': f'm_name_orig{i}'}
                    matches.rename(columns=rename_dict, inplace=True)

                    # add crsp fund name, fund number, year and month
                    matches['crsp_name_orig'] = cur_crsp['fund_name'].values
                    matches['crsp_fundno'] = cur_crsp['crsp_fundno'].values
                    matches['crsp_yearmonth'] = cur_crsp['yearmonth'].values

            elif OUTPUT_FORMAT == 'LONG':
                matches = matches.merge(cur_nd[['cik', 'fdate', 'match_name', 'fund_id', 'file_name']],
                                        how='left', left_on='m_pos', right_index=True, validate="m:1")
                rename_dict = {'cik':'m_cik', 'fdate':'m_fdate', 'match_name': 'm_name_orig',
                               'fund_id': 'm_fund_id', 'file_name': 'm_file_name'}
                matches.rename(columns=rename_dict, inplace=True)

            # write to csv file
            matches[csv_header].to_csv(os.path.join(OUTPUT_DIR, OUTPUT_NAME), mode='a', header=False, index=False)

            # notify about finish and time elapsed
            time_change = time.strftime("%H hours, %M minutes, and %S seconds",
                                        time.gmtime(time.time()-start))
            print(f"Finished matching {len(cur_crsp)} filings of {year}/{month} in {time_change}.")
