from pandas import DataFrame
import pandas as pd
import uuid


def filter_and_prepare_for_mongo(data: DataFrame):
    # Define the columns to keep and their new names
    columns_to_keep = {
        'iyear': 'year',
        'imonth': 'month',
        'iday': 'day',
        'country_txt': 'country',
        'region_txt': 'region',
        'provstate': 'province',
        'city': 'city',
        'latitude': 'latitude',
        'longitude': 'longitude',
        'summary': 'summary',
        'attacktype1_txt': 'attack_type',
        'targtype1_txt': 'target_type',
        'targsubtype1_txt': 'target_subtype',
        'natlty1_txt': 'target_nationality',
        'gname': 'group_name',
        'nperps': 'num_perpetrators',
        'weaptype1_txt': 'weapon_type',
        'nkill': 'num_killed',
        'propextent_txt': 'property_damage_extent',
        'nwound': 'num_wounded'
    }

    # Filter and rename columns
    filtered_data = data[list(columns_to_keep.keys())].rename(columns=columns_to_keep)

    filtered_data = filtered_data.drop_duplicates()

    # Prepare nested structure for MongoDB
    filtered_data['date'] = filtered_data.apply(lambda row: {
        'year': row['year'],
        'month': row['month'],
        'day': row['day']
    }, axis=1)

    filtered_data['location'] = filtered_data.apply(lambda row: {
        'country': row['country'],
        'region': row['region'],
        'province': row['province'],
        'city': row['city'],
        'latitude': row['latitude'],
        'longitude': row['longitude']
    }, axis=1)

    filtered_data['casualties'] = filtered_data.apply(lambda row: {
        'num_killed': row['num_killed'],
        'num_wounded': row['num_wounded'],
        'property_damage_extent': row['property_damage_extent']
    }, axis=1)

    filtered_data['target'] = filtered_data.apply(lambda row: {
        'type': row['target_type'],
        'subtype': row['target_subtype'],
        'nationality': row['target_nationality']
    }, axis=1)

    filtered_data['group'] = filtered_data['group_name']
    filtered_data['weapon'] = filtered_data['weapon_type']

    # Add unique ID to each record
    filtered_data['_id'] = filtered_data.apply(lambda _: str(uuid.uuid4()), axis=1)


    # Drop unnested columns
    return filtered_data.drop(columns=['year', 'month', 'day', 'country', 'region', 'province', 'city', 'latitude', 'longitude', 'num_killed', 'num_wounded', 'property_damage_extent', 'target_type', 'target_subtype', 'target_nationality', 'group_name', 'weapon_type'])
