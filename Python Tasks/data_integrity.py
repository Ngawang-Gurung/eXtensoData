import pandas as pd
import numpy as np

class DataFrameIntegrity:
    
    def __init__(self, df):
        self.df = df.copy()
        
    def null_check(self, column=None):
        self.replace_disguised_missing()
        if column:
            null_count = self.df[column].isna().sum()
            result = pd.DataFrame({column: [null_count]}, index=['null_count'])
        else:
            null_count = self.df.isna().sum()
            result = pd.DataFrame(null_count, columns=['null_count']).transpose()
        return result

    def duplicate_check(self, column):
        duplicate_count = self.df.duplicated(subset=[column]).sum()
        if duplicate_count != 0:
            print(f"\nNo. of duplicated records in column {column} is {duplicate_count}")
        
        duplicate_records = self.df[self.df.duplicated(subset=[column], keep=False)]
        return duplicate_records
    
    def negative_check(self, column):
        numeric_column = pd.to_numeric(self.df[column], errors='coerce')
        negative_records = self.df[numeric_column < 0]
        negative_count = negative_records.shape[0]
        if not negative_records.empty:
            print(f"\n{negative_count} Negative values found in column {column}")
        return negative_records

    def show_missing_records(self):
        self.replace_disguised_missing()
        return self.df[self.df.isna().any(axis=1)]
    
    def find_duplicate_row_count(self):
        return self.df.duplicated().sum()
    
    def type_check(self, column, data_type):
        invalid_records = pd.DataFrame()
        
        if data_type == 'int':
            invalid_records = self.df[self.df[column].apply(lambda x: not isinstance(x, int))]
            invalid_counts = invalid_records.shape[0]
            print(f"\n{invalid_counts} Non-integer values found in column {column}")
        elif data_type == 'str':
            invalid_records = self.df[self.df[column].apply(lambda x: not isinstance(x, str))]
            invalid_counts = invalid_records.shape[0]
            print(f"\n{invalid_counts} Non-string values found in column {column}")
        elif data_type == 'datetime':
            try:
                self.df[column] = pd.to_datetime(self.df[column], errors='raise')
            except ValueError:
                invalid_records = self.df[pd.to_datetime(self.df[column], errors='coerce').isna()]
                invalid_counts = invalid_records.shape[0]
                print(f"\n{invalid_counts} Invalid date values found in column {column}")
        else:
            print("\nInvalid data type provided. Enter either 'int', 'str', or 'datetime'.")
            return
        
        return invalid_records
    
    def replace_disguised_missing(self):
        self.df.replace({'': np.nan, ' ': np.nan, 'N/A': np.nan}, inplace=True)

df = pd.DataFrame( {
    'account_number': [1, 1, -3, 'four', '' ],
    'user': ['Alice', 2, 'Charlie', np.nan, 'Ethan'],
    'DOB': ['2020-01-01', '2021-12-31', 'June 5, 2024', '2021-12-31', '!']
})

integrity_checker = DataFrameIntegrity(df)

# print(integrity_checker.null_check())
# print(integrity_checker.null_check(column='account_number'))

# print(integrity_checker.duplicate_check(column='account_number'))

# print(integrity_checker.find_duplicate_row_count())
# print(integrity_checker.show_missing_records())

# print(integrity_checker.negative_check('account_number'))

# print(integrity_checker.type_check(column='account_number', data_type='int'))
# print(integrity_checker.type_check(column='DOB', data_type='datetime'))

# Domain Constraints
expected_types = {
    'account_number': 'int' ,
    'user': 'str',
    'DOB': 'datetime'
}

for column in df.columns:
    if column in expected_types:
        print(f"{integrity_checker.type_check(column, expected_types[column])}\n")