from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader, ConfigKey
from pandas import DataFrame
from os import path
import json
import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter


config_path = path.join(get_repo_path(), 'io_config.yaml')
config = ConfigFileLoader(config_path, 'default')

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = config.get(ConfigKey.GOOGLE_SERVICE_ACC_KEY_FILEPATH)

@data_exporter
def export_data_to_google_cloud_storage(df: DataFrame, **kwargs) -> None:    
    bucket_name = 'dw_taxi_data'
    table_name='green_taxi_data_partitioned'

    root_path=f'{bucket_name}/{table_name}'

    table = pa.Table.from_pandas(df)
    gcs = pa.fs.GcsFileSystem()

    pq.write_to_dataset(
        table,
        root_path,
        partition_cols=['lpep_pickup_date'],
        filesystem = gcs
    )
