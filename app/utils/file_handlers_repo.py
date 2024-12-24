import pandas as pd


def read_csv_file(file_path: str) -> pd.DataFrame:
    try:
        return pd.read_csv(file_path, encoding='ISO-8859-1')
    except Exception as e:
        raise Exception(f"Error reading CSV file {file_path}: {str(e)}")
