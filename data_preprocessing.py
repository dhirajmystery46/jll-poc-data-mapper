import pandas as pd
from fuzzywuzzy import fuzz

def load_csv(file_path):
    """Loads a CSV file into a Pandas DataFrame."""
    return pd.read_csv(file_path)

def check_nulls(df):
    """Checks for null values in the dataset."""
    return df.isnull().sum()

def check_duplicates(df):
    """Checks for duplicate rows in the dataset."""
    return df.duplicated().sum()

def check_id_quality(df, id_column):
    """Checks if the ID column is unique and has no nulls."""
    if id_column in df.columns:
        unique_count = df[id_column].nunique()
        total_count = len(df)
        return {
            "unique_ratio": unique_count / total_count,
            "null_values": df[id_column].isnull().sum()
        }
    return None

def analyze_column_lengths(df):
    """Analyzes text column lengths for inconsistencies."""
    return {col: df[col].astype(str).apply(len).describe() for col in df.select_dtypes(include=['object']).columns}

if __name__ == "__main__":
    source_file = "source_table.csv"
    df = load_csv(source_file)
    
    print("Null values:", check_nulls(df))
    print("Duplicate rows:", check_duplicates(df))