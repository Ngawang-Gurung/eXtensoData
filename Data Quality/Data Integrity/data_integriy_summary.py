import pandas as pd
import numpy as np
import re

class DataIntegritySummary:
    
    def __init__(self, df):
        self.df = df.copy()
        self.replace_disguised_missing()

    def integrity_check(self, column):
        null_count = self.df[column].isna().sum() + self.df[column].apply(lambda x: 1 if x == 'missing' else 0).sum()
        distinct_count = self.df[column].nunique()
        negative_count = self.df[column].apply(lambda x: isinstance(x, (int, float)) and x < 0).sum()
        invalid_int_count = self.df[column].apply(lambda x: not isinstance(x, int)).sum() 
        invalid_str_count = self.df[column].apply(lambda x: not isinstance(x, str)).sum() 
        invalid_float_count = self.df[column].apply(lambda x: not isinstance(x, float)).sum() 
        invalid_date_count = pd.to_datetime(self.df[column], format='%Y-%m-%d', errors='coerce').isna().sum() 
        invalid_email_count = self.df[column].apply(lambda x: not self.valid_email(x) if isinstance(x, str) else True).sum() 
        invalid_phone_count = self.df[column].apply(lambda x: not self.valid_phone(x) if isinstance(x, int) else True).sum()

        results = {
            'null_count': null_count,
            'distinct_count': distinct_count,
            'negative_count': negative_count,
            'invalid_int_count': invalid_int_count,
            'invalid_str_count': invalid_str_count,
            'invalid_float_count': invalid_float_count,
            'invalid_date_count': invalid_date_count,
            'invalid_email_count': invalid_email_count,
            'invalid_phone_count': invalid_phone_count,           
            'has_null': 1 if null_count else 0,
            'is_distinct': 1 if distinct_count == self.df.shape[0] else 0,
            'is_int': 1 if not invalid_int_count else 0,
            'is_str': 1 if not invalid_str_count else 0,
            'is_float': 1 if not invalid_float_count else 0,
            'is_date': 1 if not invalid_date_count else 0,
            'is_email': 1 if not invalid_email_count else 0,
            'is_phone': 1 if not invalid_phone_count  else 0

        }
        return pd.DataFrame(results, index=[column])

    def valid_email(self, email):
        regex = r'^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$'
        return bool(re.fullmatch(regex, email))  

    def valid_phone(self, phone):
        regex = r'^[0-9-]{7,12}'
        return bool(re.fullmatch(regex, str(phone)))   

    def replace_disguised_missing(self):
        self.df.replace(to_replace=['', ' ', 'N/A'], value='missing', inplace=True)

def data_integrity(df):
    print(f"The shape of df is {df.shape}")
    print(f"The number of duplicated rows is {df.duplicated().sum()}")
    integrity_checker = DataIntegritySummary(df)
    summary_df = pd.concat([integrity_checker.integrity_check(column) for column in df.columns])
    return summary_df

