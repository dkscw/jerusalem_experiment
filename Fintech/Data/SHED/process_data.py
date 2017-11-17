"""
Process the SHED data file:

Map column names. Some columns represent multiple choice options, formatted as A_a, A_b, etc.
The mapper declares a prefix to attach to each column in the target and the suffixes as an ordered
list. Values for these columns are 'Yes', 'No', etc. which are mapped to booleans using a mapper
declared in this file.

For single questions, the column mapper declares the new column name and an optional value map. This
can be a dict, the value "numeric", or "yes_no" which will use the boolean mapper.

Examples of mapper entries:
{
  "I10": {
    "prefix": "volatility_reason",
    "columns": [
      "bonus",
      "commisions",
      "seasonal_employment",
      "irregular_work_schedule",
      "periods_of_unemployment",
      "investment_income",
      "other",
    ]
  },    
  "R3": {
    "name": "rent_payment",
    "values": "numeric"
  }
}
"""

import json
from collections import OrderedDict

import pandas as pd

SOURCE_SHED_DATAFILE = 'SHED_2016_Public_Data.csv.gz'
MAPPER_FILE = 'data_mapper.json'

YES_NO_VALUE_MAP = {
    u"Yes": True,
    u"No": False,
    u"Refused": False,  # Or None?
    None: False,
}


class SHEDDataMapper(object):
    """ Mapper class for SHED data """

    def __init__(self, src_df=None, mapper=None):
        "Read source dataframe and mapper, create an empty target dataframe with the same index"
        self.src_df = src_df or pd.read_csv(SOURCE_SHED_DATAFILE, compression='gzip')
        self.target_df = pd.DataFrame(index=self.src_df.index)
        self.mapper = mapper
        if self.mapper is None:
            with open(MAPPER_FILE) as mapper_json:
                self.mapper = json.load(mapper_json, object_pairs_hook=OrderedDict)

    def map_dataframe(self):
        """ Map all the columns in the mapper """
        for src_col, data in self.mapper.items():
            if data.get('ignore'):
                continue
            column_type = self.validate_mapping_data(src_col, data)
            if column_type == 'multiple_choice':
                self.map_multiple_choice_column(src_col, data)
            elif column_type == 'single_choice':
                self.map_single_choice_column(src_col, data)
            else:
                raise NotImplementedError

    def validate_mapping_data(self, src_col, data):
        """Validate column mapping data. Column is either multiple choice, in which case data must
        include 'prefix' and 'columns', or single type in which case it must include 'name' and 
        'values' """
        is_multiple_choice = 'prefix' in data and 'columns' in data
        is_single_choice = 'name' in data and 'values' in data
        if is_multiple_choice and is_single_choice:
            raise ValueError("Ambiguous column type")
        if is_multiple_choice:
            return 'multiple_choice'
        if is_single_choice:
            return 'single_choice'
        raise ValueError("{}: Cannot detect column type".format(src_col))

    def validate_all_values_included(self, src_col, value_map):
        """ validate that all the values for the source column are included in the value map. """
        # Pandas doesn't play nice with unicode. Decode explicitly.
        unicode_values = [v.decode('utf-8') for v in self.src_df[src_col].value_counts().index]
        src_vals = set(unicode_values)
        if not src_vals.issubset(set(value_map.keys())):
            raise ValueError("{}: Value not included in map".format(src_col))

    def map_multiple_choice_column(self, src_col_prefix, mapper):
        "Map a multiple choice column"
        abc = 'abcdefghijklmnopqrstuvwxyz'
        column_name_map = OrderedDict([
            ('{}_{}'.format(src_col_prefix, src_col_suffix),
             '{}_{}'.format(mapper['prefix'], target_col_suffix))
            for src_col_suffix, target_col_suffix in zip(abc[:len(mapper['columns'])], mapper['columns'])
            ])
        for src_col, target_col in column_name_map.items():
            self.validate_all_values_included(src_col, YES_NO_VALUE_MAP)
            self.target_df[target_col] = self.src_df[src_col].map(YES_NO_VALUE_MAP)

    def process_numeric_column(self, src_col):
        "Process a numeric column: set all non-numeric values to None"
        def float_or_none(s):
            try:
                return float(s)
            except ValueError:
                return None

        return self.src_df[src_col].apply(float_or_none)

    def map_single_choice_column(self, src_col, mapper):
        "Map a single question column"
        values = mapper['values']
        target_col = mapper['name']
        if values is None:  # Copy column as is
            self.target_df[target_col] = self.src_df[src_col]
            return

        if values == 'yes_no':
            values = YES_NO_VALUE_MAP
        if isinstance(values, dict):
            self.validate_all_values_included(src_col, values)
            self.target_df[target_col] = self.src_df[src_col].map(values)
            return

        if values == 'numeric':
            self.target_df[target_col] = self.process_numeric_column(src_col)
            return

        raise ValueError("{}: Values must be a dict, numeric, or yes_no".format(src_col))


def main():
    mapper = SHEDDataMapper()
    mapper.map_dataframe()
    print mapper.target_df.to_csv(index=False)

if __name__ == '__main__':
    main()
