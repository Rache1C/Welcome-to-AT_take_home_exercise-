import pandas as pd
import gzip
from datetime import datetime

def read_forex_data(file_path):
    with gzip.open(file_path, 'rt') as f:
        df = pd.read_csv(f, parse_dates=['datetime'])
    return df

def validate_raw_data(df):
    # Ensure required columns are present
    required_columns = ['datetime', 'currency_pair', 'bid', 'ask', 'volume']
    if not all(column in df.columns for column in required_columns):
        raise ValueError("Missing required columns in the dataset") 
    
    return df

def clean_data(data):
    # Drop rows with missing values
    data = data.dropna()
    # Ensure bid and ask prices are non-negative
    data = data[(data['bid'] >= 0) & (data['ask'] >= 0)]
    # Ensure data is sorted by timestamp
    data = data.sort_values(by='datetime')
    return data

def transform_data_to_minutely_ohlc(data):
    # Calculate mid price
    data['mid'] = (data['bid'] + data['ask']) / 2
    # Set timestamp as index
    data = data.set_index('datetime')
    # Resample to minutely OHLC
    ohlc_dict = {'mid': 'ohlc'}
    minutely_ohlc = data.resample('T').apply(ohlc_dict)['mid']
    # Flatten the column names
    minutely_ohlc.columns = ['open', 'high', 'low', 'close']
    minutely_ohlc.reset_index(inplace=True)
    df = pd.merge_asof(data, minutely_ohlc, on="datetime")
    return df

def refine_data(data):
    #df = data.drop(data['bid']).drop(data['ask']).drop(data['volume']).drop(data['mid'])
    df = data[data['datetime'] > pd.Timestamp('2024-01-01')]
    selected_columns = ['datetime','currency_pair', 'open', 'high', 'low', 'close']
    filtered_df = df[selected_columns]
    return filtered_df

def validate_processed_data(data):
    # Ensure there are no missing minute intervals
    if data.isnull().values.any():
        print("Warning: Processed data contains missing values.")
    # Ensure required columns are present
    required_columns = ['datetime', 'currency_pair', 'open', 'high', 'low', 'close']
    if not all(column in data.columns for column in required_columns):
        raise ValueError("Missing required columns in the dataset") 
    return data

def write_to_csv(df, output_file):
    df.to_csv(output_file, index=False, compression='gzip' if output_file.endswith('.gz') else None)


file_list = ['sample_fx_data_A.csv.gz']
    
for file in file_list:
    output_file = 'processed_'+file
    # Step 1: Read the Forex dataset
    raw_data = read_forex_data(file)
    
    # Step 2: Validate the raw data
    validated_data = validate_raw_data(raw_data)
    
    # Step 3: Clean the data
    cleaned_data = clean_data(validated_data)
    
    # Step 4: Transform the tick data to minutely OHLC data
    ohlc_data = transform_data_to_minutely_ohlc(cleaned_data)

    #Step 5: Refine the data to only include records from 2024 onwards
    refined_ohlc_data = refine_data(ohlc_data)
    
    # Step 6: Validate the processed data
    validated_ohlc_data = validate_processed_data(refined_ohlc_data)
    
    # Step 7: Write the results to an output file
    write_to_csv(validated_ohlc_data, output_file )
