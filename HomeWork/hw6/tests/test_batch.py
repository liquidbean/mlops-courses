import pandas as pd
from datetime import datetime
import os 
import batch

S3_ENDPOINT_URL = os.environ.get('S3_ENDPOINT_URL')

def dt(hour, minute, second=0):
    return datetime(2023, 1, 1, hour, minute, second)

def test_prepare_data():
    data = [
        (None, None, dt(1, 1), dt(1, 10)),
        (1, 1, dt(1, 2), dt(1, 10)),
        (1, None, dt(1, 2, 0), dt(1, 2, 59)),
        (3, 4, dt(1, 2, 0), dt(2, 2, 1)),      
    ]
    columns = ['PULocationID', 'DOLocationID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime']
    categorical = ['PULocationID', 'DOLocationID']
    df = pd.DataFrame(data, columns=columns)
    
    df_expected = pd.DataFrame({
        'PULocationID': ['-1', '1', '1', '3'],
        'DOLocationID': ['-1', '1', '-1', '4'],
        'tpep_pickup_datetime': [dt(1, 1), dt(1, 2), dt(1, 2, 0), dt(1, 2, 0)],
        'tpep_dropoff_datetime': [dt(1, 10), dt(1, 10), dt(1, 2, 59), dt(2, 2, 1)],
        'duration': [9.0, 8.0, 0.983333, 61.016667]
    })
    
    df_prepared = batch.prepare_data(df, categorical)

    df_expected = df_expected[(df_expected['duration'] >= 1) & (df_expected['duration'] <= 60)]

    assert df_prepared.to_dict(orient='records') == df_expected.to_dict(orient='records')


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

    os.environ['OUTPUT_FILE_PATTERN'] = "s3://nyc-duration/out/{year:04d}-{month:02d}.parquet"

    os.system("python ../batch.py 2023 1")

    output_file = batch.get_output_path(year=2023, month=1)

    df_output = pd.read_parquet(output_file, storage_options=options)

    assert df_output.shape[0] == 2
    assert df_output['predicted_duration'].sum() == 17.083333

if __name__ == '__main__':
    integration_test()