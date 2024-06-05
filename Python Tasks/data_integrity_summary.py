import pandas as pd
import numpy as np

from utils.sql_connection import table_df

class DataFrameIntegritySummary:
    
    def __init__(self, df):
        self.df = df.copy()
        self.replace_disguised_missing()
        
    def replace_disguised_missing(self):
        self.df.replace({'': np.nan, ' ': np.nan, 'N/A': np.nan}, inplace=True)
        
    def null_check(self, column):
        return self.df[column].isna().sum()

    def duplicate_check(self, column):
        return self.df.duplicated(subset=[column]).sum()

    def negative_check(self, column):
        numeric_column = pd.to_numeric(self.df[column], errors='coerce')
        return numeric_column[numeric_column < 0].count()

    def type_check(self, column, data_type):
        if data_type == 'int':
            return self.df[column].apply(lambda x: not isinstance(x, int)).sum()
        elif data_type == 'str':
            return self.df[column].apply(lambda x: not isinstance(x, str)).sum()
        elif data_type == 'datetime':
            try:
                pd.to_datetime(self.df[column], errors='raise')
                return 0
            except (ValueError, TypeError):
                return pd.to_datetime(self.df[column], errors='coerce').isna().sum()
        else:
            raise ValueError("Invalid data type provided. Enter either 'int', 'str', or 'datetime'.")

    def integrity_check_df(self, column, expected_type):
        results = {
            'null': self.null_check(column),
            'duplicate': self.duplicate_check(column),
            'negative': self.negative_check(column),
            'invalid_data_types': self.type_check(column, expected_type)
        }
        return pd.DataFrame(results, index=[column])

df = table_df(database_name='customer', table_name='new_customer_profile')

integrity_checker = DataFrameIntegritySummary(df)

summary_df = pd.DataFrame()

for column in df.columns:
    result = integrity_checker.integrity_check_df(column, 'str')
    summary_df = pd.concat([summary_df, result])

print(summary_df)
