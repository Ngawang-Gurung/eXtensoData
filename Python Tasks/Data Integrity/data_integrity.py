import pandas as pd
import numpy as np

class DataIntegrity:
    
    def __init__(self, df):
        self.df = df.copy()
        self.replace_disguised_missing()

    def null_check(self, column = None):
        if column:
            null_count = self.df[column].isna().sum()
            null_records = self.df[self.df[column].isna()]
        else:
            null_count = self.df.isna().sum().sum()
            null_records = self.df[self.df.isna().any(axis=1)]

        if null_count !=0:
            if column:
                print(f"\n {null_count} null values found in column {column}\n")
            else:
                print(f"\n{null_count} null records found in the dataframe\n")

        return null_records
    
    def duplicate_check(self, column = None):
        if column:
            duplicate_count = self.df.duplicated(subset=[column]).sum()
            duplicate_records = self.df[self.df.duplicated(subset=[column], keep=False)]
        else:
            duplicate_count = self.df.duplicated().sum()
            duplicate_records = self.df[self.df.duplicated(keep=False)]

        if duplicate_count != 0:
            if column:
                print(f"\n{duplicate_count} duplicate values found in column '{column}'\n")
            else:
                print(f"\n{duplicate_count} duplicate records found in the dataframe\n") 

        return duplicate_records
    
    def negative_check(self, column):
        numeric_column = pd.to_numeric(self.df[column], errors='coerce')
        negative_records = self.df[numeric_column < 0]
        negative_count = negative_records.shape[0]
        if not negative_records.empty:
            print(f"\n{negative_count} negative values found in column {column}\n")
        return negative_records

    def type_check(self, column, data_type):
        invalid_records = pd.DataFrame()
        invalid_counts = 0

        if data_type == 'int':
            invalid_records = self.df[self.df[column].apply(lambda x: not isinstance(x, int))]
            invalid_counts = invalid_records.shape[0]
        elif data_type == 'str':
            invalid_records = self.df[self.df[column].apply(lambda x: not isinstance(x, str))]
            invalid_counts = invalid_records.shape[0]
        elif data_type == 'datetime':
            try:
                self.df[column] = pd.to_datetime(self.df[column], errors='raise')
            except ValueError:
                invalid_records = self.df[pd.to_datetime(self.df[column], errors='coerce').isna()]
                invalid_counts = invalid_records.shape[0]
        else:
            print("\nInvalid data type provided. Enter either 'int', 'str', or 'datetime'.")
            return None
       
        if invalid_counts:
            print(f"\n{invalid_counts} invalid '{data_type}' values found in column '{column}'\n")
        
        return invalid_records
    
    def replace_disguised_missing(self):
        self.df.replace({'': np.nan, ' ': np.nan, 'N/A': np.nan}, inplace=True)

df = pd.DataFrame( {
    'account_number': [1, 1, -3, 'four', '' ],
    'user': ['Alice', 2, 'Charlie', np.nan, 'Ethan'],
    'DOB': ['2020-01-01', '2021-12-31', 'June 5, 2024', '2021-12-31', '!']
})

expected_types = {
    'account_number': 'int' ,
    'user': 'str',
    'DOB': 'datetime'
}

integrity_checker = DataIntegrity(df)

# print(integrity_checker.null_check())
print(integrity_checker.null_check(column='account_number'))

# print(integrity_checker.duplicate_check())
print(integrity_checker.duplicate_check(column='account_number'))

print(integrity_checker.negative_check('account_number'))

print(integrity_checker.type_check(column='account_number', data_type='int'))
print(integrity_checker.type_check(column='DOB', data_type='datetime'))