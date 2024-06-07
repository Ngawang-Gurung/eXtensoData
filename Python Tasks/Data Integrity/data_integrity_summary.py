import pandas as pd
import numpy as np
import re

class DataIntegritySummary:
    
    def __init__(self, df):
        self.df = df.copy()
        self.replace_disguised_missing()

    def integrity_check(self, column):
        null_count = self.df[column].isna().sum() + self.df[column].apply(lambda x: 1 if x == 'missing' else 0).sum()
        duplicate_count = self.df.duplicated(subset=[column]).sum()
        negative_count = self.df[column].apply(lambda x: 1 if isinstance(x, (int, float)) and x < 0 else 0).sum()
        invalid_int_count = self.df[column].apply(lambda x: not isinstance(x, int)).sum()
        invalid_str_count = self.df[column].apply(lambda x: not isinstance(x, str)).sum()
        invalid_float_count = self.df[column].apply(lambda x: not isinstance(x, float)).sum()
        invalid_date_count = pd.to_datetime(self.df[column], format='%Y-%m-%d', errors='coerce').isna().sum()
        email_count = self.df[column].apply(lambda x: self.valid_email(x) if isinstance(x, str) else False).sum()
        phone_count = self.df[column].apply(lambda x: self.valid_phone(x) if isinstance(x, int) else False).sum() 
        
        results = {
            'null_count': null_count,
            'duplicate_count': duplicate_count,
            'negative_count': negative_count,
            'invalid_int_count': invalid_int_count,
            'invalid_str_count': invalid_str_count,
            'invalid_float_count': invalid_float_count,
            'invalid_date_count': invalid_date_count,
            'email_count': email_count,
            'phone_count': phone_count,
            'has_null': 1 if null_count else 0,
            'has_duplicate': 1 if duplicate_count else 0,
            'is_int': 1 if invalid_int_count == 0 else 0,
            'is_str': 1 if invalid_str_count == 0 else 0,
            'is_float': 1 if invalid_float_count == 0 else 0,
            'is_date': 1 if invalid_date_count == 0 else 0,
            'is_email': 1 if email_count == self.df.shape[0] else 0,
            'is_phone': 1 if phone_count == self.df.shape[0] else 0
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

df = pd.DataFrame( {
    'account_number': [1, 1, -3.0, 'four', '' ],
    'user': ['Alice', 2, 'Charlie', np.nan, 'Ethan'],
    'DOB': ['2020-01-01', '2021-12-31', 'June 5, 2024', '2021-12-31', '!'],  
    'email': ['alice@mail.com', '', 'charlie@mail.com', 'david@example.com', 'notamaile@com'],
    'phone_number': [101212121, '', 12121122, 7777777, 712232132]
})

integrity_checker = DataIntegritySummary(df)
summary_df = pd.concat([integrity_checker.integrity_check(column) for column in df.columns])
print(summary_df)
