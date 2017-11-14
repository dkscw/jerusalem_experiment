import numpy as np
import pandas as pd
import csv
import re

CODEBOOK_TXT_FILENAME = 'shed_2016codebook.txt'
CODEBOOK_CSV_FILENAME = 'shed_2016codebook.csv'
SOURCE_DATA_FILENAME = 'SHED_2016_Public_Data.csv'
UNSTACKED_DATA_FILENAME = 'SHED_2016_Public_Data.p'

def parse_codebook(codebook):
    "Parse the codebook, passed as a string"
    # Remove footers
    footer_pattern = 'Page [0-9]+\n\n\x0cCodebook_2016_SHED.txt'
    codebook, n = re.subn(footer_pattern, '', codebook)
    print "Dropped {} footers".format(n)

    # Lines are 79 characters long. Variable definitions are enclosed between
    # lines of dashes.
    lines = [l for l in codebook.split('-' * 79) if len(l) > 0]
    # Get rid of the preamble and get the variable definitions
    var_defs = lines[3::2]
    # Split into variable and definition
    def get_var(raw_var):
        "drop trailing spaces"
        return raw_var.strip()

    def get_def(raw_def):
        "concatenate lines"
        return raw_def.replace('\n', ' ')

    var_defs = [vd.split('\n', 1) for vd in var_defs]
    return [{'variable': get_var(vd[0]), 'definition': get_def(vd[1])} for vd in var_defs]

def unstack_dataframe(df, codebook_df):
    """ unstack variables with multiple choice options: replace variables that look like A_a, A_b 
    whose options are Yes/No with a single variable whose value is the list of all 'Yes' options """
    def get_var_option(var):
        definition = codebook_df.loc[var]['definition']
        # The option is within brackets that begin the variable definition
        return definition.split(']')[0][1:]

    multiple_choice_options = {'Yes', 'No', 'Refused', 'Don\x92t know', np.nan}
    to_replace = [v for v in df.columns if '_' in v and set(df[v]).issubset(multiple_choice_options)]

    # Create new columns
    new_vars = set([v.split('_')[0] for v in to_replace])
    for v in new_vars:
        df[v] = np.empty((len(df), 0)).tolist()  # initialize with empy lists

    for v in to_replace:
        op = get_var_option(v)
        print "Processing {}: {}".format(v, op)
        new_var = v.split('_')[0]
        option_col = df[v].apply(lambda x: [op] if x == 'Yes' else [])
        df[new_var] += option_col
        df.drop(v, axis=1, inplace=True)

    return df


def generate_codebook_csv():
    with open(CODEBOOK_TXT_FILENAME) as f:
        codebook = parse_codebook(f.read())

    with open(CODEBOOK_CSV_FILENAME, 'wb') as csvfile:
        fieldnames = ['variable', 'definition']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for var_def in codebook:
            writer.writerow(var_def)


def generate_unstacked_dataframe():
    df = pd.read_csv(SOURCE_DATA_FILENAME)
    codebook_df = pd.read_csv(CODEBOOK_CSV_FILENAME).set_index('variable')
    return unstack_dataframe(df, codebook_df) 


if __name__ == '__main__':
    df = generate_unstacked_dataframe()
    df.to_pickle(UNSTACKED_DATA_FILENAME)
