import pandas as pd
from fuzzywuzzy import fuzz
from column_mapping import match_columns  # Assuming this is from your existing code

# ... (Keep your existing imports and functions)

def load_csv_data(file):
    return pd.read_csv(file)

def match_tables(source_df, target_df):
    mappings = {}
    for source_table in source_df['Table Name'].unique():
        source_data = source_df[source_df['Table Name'] == source_table]
        scores = []
        for target_table in target_df['Table Name'].unique():
            target_data = target_df[target_df['Table Name'] == target_table]
            score = 0
            for _, source_row in source_data.iterrows():
                best_match_score = 0
                for _, target_row in target_data.iterrows():
                    match_score = (
                        fuzz.ratio(str(source_row['Field']), str(target_row['Field'])) +
                        fuzz.ratio(str(source_row['Business Definition']), str(target_row['Business Definition'])) +
                        fuzz.ratio(str(source_row['Data_Type']), str(target_row['Data_Type'])) +
                        fuzz.ratio(str(source_row['Values']), str(target_row['Values']))
                    ) / 4
                    best_match_score = max(best_match_score, match_score)
                score += best_match_score
            scores.append((target_table, score / len(source_data)))
        scores.sort(key=lambda x: x[1], reverse=True)
        mappings[source_table] = scores[:3]
    return mappings