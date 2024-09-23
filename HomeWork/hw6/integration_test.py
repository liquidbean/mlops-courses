import pandas as pd
from datetime import datetime
import os 
import batch

S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def integration_test():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    df_input = pd.DataFrame(data, columns=columns)

    os.environ['INPUT_FILE_PATTERN'] = "s3://nyc-duration/in/{year:04d}-{month:02d}.parquet"

    input_file = batch.get_input_path(year=2023, month=1)

    options = {'client_kwargs': {'endpoint_url': S3_ENDPOINT_URL}} if S3_ENDPOINT_URL else None

    df_input.to_parquet(
        input_file,
        engine='pyarrow',
        compression=None,
        index=False,
        storage_options=options
    )

if __name__ == '__main__':
    integration_test()