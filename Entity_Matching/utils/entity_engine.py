import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
from Levenshtein import distance
from datetime import datetime

def combine_layouts(A, B, metric='levenshtein', threshold=20):
    def calculate_similarity(A, B, metric):
        if metric == 'cosine':
            tfidf = TfidfVectorizer(stop_words='english')
            combined_soup = pd.concat([A['soup'], B['soup']], ignore_index=True)
            tfidf.fit(combined_soup)
            tfidf_matrix_A = tfidf.transform(A['soup'])
            tfidf_matrix_B = tfidf.transform(B['soup'])
            similarity = cosine_similarity(tfidf_matrix_A, tfidf_matrix_B)
            similarity_df = pd.DataFrame(similarity, index=A.index, columns=B.index)
            idx_row = similarity_df.idxmax(axis=1)
            similarity_mask = similarity_df.max(axis=1) > threshold
        else:
            distance_matrix = pd.DataFrame([[distance(a, b) for b in B['soup']] for a in A['soup']], index=A.index,
                                           columns=B.index)
            idx_row = distance_matrix.idxmin(axis=1)
            similarity_mask = distance_matrix.min(axis=1) <= threshold
        return idx_row, similarity_mask

    def merge_data(A, B, idx_row, similarity_mask):
        combined_columns = list(set(A.columns) | set(B.columns))
        combined_data = pd.DataFrame(columns=combined_columns)
        for idx_A in A.index:
            if similarity_mask[idx_A]:
                idx_B = idx_row[idx_A]
                combined_row = A.loc[idx_A].combine_first(B.loc[idx_B])
                combined_row['source'] = f"{A.loc[idx_A]['source']}, {B.loc[idx_B]['source']}"
                combined_row['modified_date'] = datetime.now()
            else:
                combined_row = A.loc[idx_A]
            combined_data = pd.concat([combined_data, combined_row.to_frame().T], ignore_index=True)
        new_records = B.loc[~B.index.isin(idx_row[similarity_mask].values)]
        return pd.concat([combined_data, new_records], ignore_index=True)

    idx_row, similarity_mask = calculate_similarity(A, B, metric)
    return merge_data(A, B, idx_row, similarity_mask)