import pandas as pd
from datetime import datetime
import os
import batch

file_path=os.getenv('INPUT_FILE_PATTERN')
S3_ENDPOINT_URL = os.getenv('S3_ENDPOINT_URL', 'http://localhost:4566')
OUTPUT_FILE_PATTERN=os.getenv('OUTPUT_FILE_PATTERN')

year = 2022
month = 1

print(f'Running batch.py...')
os.system(f"python batch.py {year} {month}")

data = batch.read_data(OUTPUT_FILE_PATTERN)
print(data)

# def dt(hour, minute, second=0):
#     return datetime(2022, 1, 1, hour, minute, second)

# df = pd.DataFrame({
#         'PULocationID': ['-1', '1', '1'], 
#         'DOLocationID': ['-1', '-1', '2'],
#         'tpep_pickup_datetime': [dt(1, 2), dt(1, 2), dt(2, 2)],
#         'tpep_dropoff_datetime': [dt(1, 10), dt(1, 10), dt(2, 3)],
#         'duration': [8.0, 8.0, 1.0]
#     })

# options = {
#     'client_kwargs': {
#     'endpoint_url': S3_ENDPOINT_URL
#     }
# }

# df.to_parquet(
#     file_path,
#     engine='pyarrow',
#     compression=None,
#     index=False,
#     storage_options=options
# )