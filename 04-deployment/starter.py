import pickle
import pandas as pd
import argparse

def load_model():
    with open('model.bin', 'rb') as f_in:
        dv, model = pickle.load(f_in)
    return dv, model


def read_data(input_file, year, month):
    df = pd.read_parquet(input_file)
    
    df['duration'] = df.tpep_dropoff_datetime - df.tpep_pickup_datetime
    df['duration'] = df.duration.dt.total_seconds() / 60

    df = df[(df.duration >= 1) & (df.duration <= 60)].copy()

    categorical = ['PULocationID', 'DOLocationID']

    df[categorical] = df[categorical].fillna(-1).astype('int').astype('str')
    
    df['ride_id'] = f'{year:04d}/{month:02d}_' + df.index.astype('str')
    
    return df


def prepare_dictionaries(df: pd.DataFrame):
    categorical = ['PULocationID', 'DOLocationID']
    numerical = ['trip_distance']
    dicts = df[categorical + numerical].to_dict(orient='records')
    return dicts

def save_results(df, y_pred, output_file):
    df_result = pd.DataFrame()
    df_result['ride_id'] = df['ride_id']
    df_result['PULocationID'] = df['PULocationID']
    df_result['DOLocationID'] = df['DOLocationID']
    df_result['actual_duration'] = df['duration']
    df_result['predicted_duration'] = y_pred
    df_result['mean_duration'] = y_pred.mean()
    print(f'Mean predicted duration: {y_pred.mean():.02f}')

    df_result.to_parquet(output_file, index=False)

def apply_model(year, month, input_file, output_file):

    df = read_data(input_file, year, month)
    dicts = prepare_dictionaries(df)
    dv, model = load_model()
    X_val = dv.transform(dicts)
    y_pred = model.predict(X_val)

    save_results(df, y_pred, output_file)

    return output_file




def ride_duration_prediction(year, month, taxi_type):

    input_file = f'https://d37ci6vzurychx.cloudfront.net/trip-data/{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet'
    output_file = f's3://nyc-duration-prediction-zhukavets/{taxi_type}_predicted_data_{year:04d}-{month:02d}.parquet'

    apply_model(year, month, input_file, output_file)


def run():
    parser = argparse.ArgumentParser()
    parser.add_argument('year', type=int, help="Year to predict")
    parser.add_argument('month', type=int, help="Month to predict")
    parser.add_argument('taxi_type', type=str, help="Taxi type to predict")
    args = parser.parse_args()

    year = args.year
    month = args.month
    taxi_type = args.taxi_type

    ride_duration_prediction(year, month, taxi_type)

if __name__ == '__main__':
    run()