from pandas import DataFrame
import pandas as pd
import uuid
from typing import Dict, Any
from datetime import datetime


def parse_date(date_str: str) -> Dict[str, Any]:
    try:
        parsed_date = datetime.strptime(date_str, "%d-%b-%y")
        return {"year": parsed_date.year, "month": parsed_date.month, "day": parsed_date.day}
    except Exception:
        return {"year": None, "month": None, "day": None}


def filter_and_prepare_for_mongo_second_csv(data: DataFrame):
    columns_to_keep = {
        'Date': 'date',
        'City': 'city',
        'Country': 'country',
        'Perpetrator': 'group_name',
        'Weapon': 'weapon_type',
        'Injuries': 'num_wounded',
        'Fatalities': 'num_killed',
        'Description': 'summary'
    }

    filtered_data = data[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

    filtered_data = filtered_data.drop_duplicates()

    filtered_data['date'] = filtered_data.apply(lambda row: parse_date(row['date']), axis=1)

    filtered_data['location'] = filtered_data.apply(lambda row: {
        'country': row['country'],
        'region': "Unknown",
        'province': {"$numberDouble": "NaN"},
        'city': row['city'] if pd.notna(row['city']) else "Unknown",
        'latitude': None,
        'longitude': None
    }, axis=1)
    print(2)

    filtered_data['casualties'] = filtered_data.apply(lambda row: {
        'num_killed': row['num_killed'] if pd.notna(row['num_killed']) else 0,
        'num_wounded': row['num_wounded'] if pd.notna(row['num_wounded']) else 0,
        'property_damage_extent': {"$numberDouble": "NaN"}
    }, axis=1)

    filtered_data['target'] = filtered_data.apply(lambda row: {
        'type': "Unknown",
        'subtype': "Unknown",
        'nationality': row['country']
    }, axis=1)

    filtered_data['group'] = filtered_data.apply(
        lambda row: row['group_name'] if pd.notna(row['group_name']) else "Unknown", axis=1)
    filtered_data['weapon'] = filtered_data.apply(
        lambda row: row['weapon_type'] if pd.notna(row['weapon_type']) else "Unknown", axis=1)

    filtered_data['_id'] = filtered_data.apply(lambda _: str(uuid.uuid4()), axis=1)

    return filtered_data.drop(columns=list(columns_to_keep.values()))