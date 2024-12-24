from app.config.local_files_config import GLOBAL_TERRORISM, WORLDWIDE_TERRORISM_INCIDENTS
from typing import Dict, Any, Callable, List, Generator

from app.service.normalize import filter_and_prepare_for_mongo
from app.service.normalize_second_csv import filter_and_prepare_for_mongo_second_csv
from app.utils.file_handlers_repo import read_csv_file
from app.service.kafka_handlers import publish_batch
from dotenv import load_dotenv
from functools import partial
import pandas as pd
import os

load_dotenv(verbose=True)


def create_batches(data: List[Dict[str, Any]], batch_size: int = 100) -> Generator[List[Dict[str, Any]], None, None]:
    for i in range(0, len(data), batch_size):
        yield data[i:i + batch_size]


def process_csv_data(
        file_path: str,
        topic: str,
        id_field: str,
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x
):
    df = read_csv_file(file_path)
    filter_df = filter_and_prepare_for_mongo(df)
    data = filter_df.to_dict('records')

    for batch in create_batches(data):
        transformed_batch = [transform_func(record) for record in batch]
        publish_batch(topic, transformed_batch, id_field)


def process_second_csv_data(
        file_path: str,
        topic: str,
        id_field: str,
        transform_func: Callable[[Dict[str, Any]], Dict[str, Any]] = lambda x: x
):
    df = read_csv_file(file_path)
    filter_df = filter_and_prepare_for_mongo_second_csv(df)
    data = filter_df.to_dict('records')

    for batch in create_batches(data):
        transformed_batch = [transform_func(record) for record in batch]
        publish_batch(topic, transformed_batch, id_field)


process_global_terrorism = partial(
    process_csv_data,
    file_path=GLOBAL_TERRORISM,
    topic=os.environ['GLOBAL_TERRORISM'],
    id_field='_id'
)

process_global_terrorism_second_csv = partial(
    process_second_csv_data,
    file_path=WORLDWIDE_TERRORISM_INCIDENTS,
    topic=os.environ['GLOBAL_TERRORISM'],
    id_field='_id'
)
