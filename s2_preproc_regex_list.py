'''
File: s4_2_preproc_regex_list.py
Project: Extract Location

File Created: Monday, 16th November 2020 1:20:40 pm

Author: Georgij Alekseev (geo.ale104@gmail.com)
-----
Last Modified: Wednesday, 12th January 2022 9:54:26 pm
-----
Description: The file contains the list of regular expressions that will be applied.
The whole purpose of this file is to free up space in 1_fuzzy_match.py
'''
# word replacements in the format: ('word', 'replacement')
wrd_rplc = [('intl', 'international'),
            ('mgd', 'managed'),
            ('capitalization', 'cap'),
            ('co', 'company'),
            ('ca', 'california'),
            ('govt', 'government'),
            ('infla', 'inflation'),
            ('de', 'delaware'),
            ('lm', 'legg mason'),
            ('mid cap', 'midcap'),
            ('tax free', 'taxfree'),
            ('franklin temp', 'franklin templeton'),
            ('rtrn', 'return'),
            ('multi state', 'multistate'),
            ('short term', 'shortterm'),
            ('long term', 'longterm'),
            ('tax exempt', 'taxexempt'),
            ('high income', 'highincome'),
            ('nicholas applegate', 'nicholasapplegate'),
            ('ltd', 'limited'),
            ('fixed income', 'fixedincome'),
            ('phoenix oakhurst', 'phoenixoakhurst'),
            ('income growth', 'incomegrowth'),
            ('tiaa cref', 'tiaacref'),
            ('multi portfolio', 'multiportfolio'),
            ('tax managed', 'taxmanaged'),
            ('dreyfus laurel', 'dreyfuslaurel'),
            ('variable insurance products', 'variable insurance products vip'),
            ('transamerica', 'transamerica ta'),
            ('trans.', 'transamerica'),
            ('goldman sachs', 'goldman sachs gs'),
            ('eaton vance', 'eaton vance ev'),
            ('rydex variable', 'rydex variable va'),
            ('small cap', 'smallcap'),
            ('large cap', 'largecap'),
            ('alloc', 'allocation'),
            ('mkt', 'market'),
            ('mkts', 'markets'),
            ('markets', 'market'),
            ('franklin templeton', 'franklin templeton ft'),
            ('inflation protected', 'inflationprotected'),
            ('sei institutional managed', 'sei institutional managed simt'),
            ('navellier millennium', 'navellier millennium tiger'),
            ('amt free', 'amtfree'),
            ('alps alerian', 'alpsalerian'),
            ('tax sensitive', 'taxsensitive'),
            ('exchange traded', 'exchangetraded'),
            ('jp morgan', 'jpmorgan'),
            ('u s', 'us'),
            ('s p', 'sp'),
            ('fidelity advisor', 'fidelity'),
            ('port|por|portf', 'portfolio'),
            ('mm|mmkt', 'money market'),
            ('massmutual', 'massmutual mml'),
            ('smith barney', 'smith barney sb shearson'),
            ('muni', 'municipal'),
            ('multi cap', 'multicap'),
            ('loomis sayles', 'natixis loomis sayles'),
            ('"a"|"b"|"c"', ''),
            ('high yield', 'highyield'),
            ('tax advantage|tax advantaged', 'taxadvantage'),
            ('nasdaq 100', 'nasdaq100'),
            ('val', 'value'),
            ('health care', 'healthcare'),
            ('investments', 'investment'),
            ('assets', 'asset'),
            ('spdr', 'spdr spdrr'),
            ('pa', 'pennsylvania'),
            ('yld', 'yield'),
            ('pimco', 'pimco pims'),
            ('babson d l', 'babson dl'),
            ('stewart w p', 'stewart wp'),
            ('grwth', 'growth'),
            ('eq', 'equity'),
            ('western asset', 'western asset wa waf'),
            ('dj', 'dow jones'),
            ('srs', 'series'),
            ('blackrock', 'br'),
            ('old mutual', 'old mutual om'),
            ('mfs variable insurance', 'mfs variable insurance vit'),
            ('alps variable insurance', 'alps variable insurance avs'),
            ('ivy variable insurance', 'ivy variable insurance vip'),
            ('strategic partners mutual', 'strategic partners mutual sp'),
            ('interm|inter|intermed', 'intermediate'),
            ('wells fargo', 'wells fargo advantage')]

# surround first 'word' with non-characters to avoid capturing fragments within a longer word
wrd_rplc = [(rf'(^|[^a-zA-Z])({word})([^a-zA-Z]|$)', rf'\1{sub}\3') for word, sub in wrd_rplc]

# regular sub expressions that will be applied with re.sub on string s
# e.g., REGEX_SUB = [(f'/.{2,3}/\s*$', '')] removes /MA/ at end of the line
REGEX_SUB = [("'", ''),
             (r'(/\S{2,3}/|/\S{1,5})\s*$', ''),         # remove /MA/ or /MA at end of line
             (r'\.([a-zA-Z]([^a-zA-Z]|$))', r'\1'),     # remove period if followed by single letter, e.g. 'U.S'  'US'
             (r'\.([a-zA-Z]([^a-zA-Z]|$))', r'\1'), # applying it twice to account for, e.g., V.I.S.
             (r'&amp;', ''),
             (r'\(formerly.*?\)', ''),
             (r'/', ' '),
             (r'\.', ' ')]                          # replace period with space

REGEX_SUB = REGEX_SUB + wrd_rplc
