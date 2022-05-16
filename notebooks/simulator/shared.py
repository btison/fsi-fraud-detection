import os
import sys
import json
import time
import requests
import datetime
import pandas as pd


def read_from_pkl(input_dir, begin_date, end_date):

    files = [os.path.join(input_dir, f) for f in os.listdir(
        input_dir) if f >= begin_date+'.pkl' and f <= end_date+'.pkl']

    frames = []
    for f in files:
        try:
            df = pd.read_pickle(f)
            frames.append(df)
            del df
        except:
            print(f" --> skipping file '{f}'")

    df_final = pd.concat(frames)

    df_final = df_final.sort_values('TRANSACTION_ID')
    df_final.reset_index(drop=True, inplace=True)
    #  Note: -1 are missing values for real world data
    df_final = df_final.replace([-1], 0)

    return df_final


def read_from_csv(input_dir, begin_date, end_date):

    files = [os.path.join(input_dir, f) for f in os.listdir(
        input_dir) if f >= begin_date+'_fraud.csv' and f <= end_date+'_fraud.csv']

    frames = []
    for f in files:
        try:
            df = pd.read_csv(f)
            frames.append(df)
            del df
        except:
            print(f" --> skipping file '{f}'")

    df_final = pd.concat(frames)

    df_final = df_final.sort_values('TRANSACTION_ID')
    df_final.reset_index(drop=True, inplace=True)
    #  Note: -1 are missing values for real world data
    df_final = df_final.replace([-1], 0)

    return df_final


def merge_csv_files(file_collection):
    frames = []
    for f in file_collection:
        df = df = pd.read_csv(f)
        frames.append(df)
        del df

    df_final = pd.concat(frames)
    df_final = df_final.sort_values('TX_DATETIME')
    df_final.reset_index(drop=True, inplace=True)

    return df_final


def load_transactions(file_collection, cutoff_date=None, time_window=-1):
    tx_df = merge_csv_files(file_collection)

    # TX_DATETIME is in UNIX time in the files. This needs conversion for more advanced calculations, e.g. rolling time-windows
    tx_df['TX_DATETIME'] = pd.to_datetime(tx_df['TX_DATETIME'] * 1000000000)

    if cutoff_date == None and time_window == -1:
        return tx_df  # return all transactions

    if cutoff_date != None and time_window == -1:
        return transactions_df.loc[transactions_df['TX_DATETIME'] > cutoff_date.strftime("%Y-%m-%d")]

    if cutoff_date == None and time_window > -1:
        # return the n last days
        d = tx_df['TX_DATETIME'].max(
        ) - datetime.timedelta(days=time_window + 1)
        return tx_df.loc[tx_df['TX_DATETIME'] > d.strftime("%Y-%m-%d")]

    # if cutoff_date != None and time_window > -1:
    # this is not supported yet!

    return td_df


def upload_transactions(bridge, topic='tx-sim', start='2020-05-01', end='2020-05-01', loc='./data/simulated/pkl/', batch_size=100):

    KAFKA_ENDPOINT = f"{bridge}/topics/{topic}"
    KAFKA_HEADERS = {'content-type': 'application/vnd.kafka.json.v2+json'}

    # read the raw transaction data
    transactions_df = read_from_pkl(loc, start, end)
    NUM_TX = len(transactions_df)

    batch = []

    for index, row in transactions_df.iterrows():
        batch.append(row)

        if len(batch) % batch_size == 0:
            payload = {"records": []}

            for r in batch:
                record = {'value': r.to_json()}
                payload['records'].append(record)

            # post the payload with backoff/retry in case the bridge gets overloaded ...
            try:
                success = False
                retry = 0

                while not success:
                    r = requests.post(
                        KAFKA_ENDPOINT, headers=KAFKA_HEADERS, json=payload)
                    if r.status_code == 200:
                        success = True
                    else:
                        retry = retry + 1
                        if retry > 5:
                            print('aborting...')
                            sys.exit()
                        time.sleep(retry * 2)
                        print(f"backing-off/retry {retry}/5")
            except:
                print('exception/aborting...')
                sys.exit()

            batch = []
            print(f" --> uploaded {index+1}/{NUM_TX}")


def upload_fraud(bridge, topic='tx-fraud-sim', start='2020-04-01', end='2020-04-01', loc='./data/simulated/fraud/', batch_size=100):

    KAFKA_ENDPOINT = f"{bridge}/topics/{topic}"
    KAFKA_HEADERS = {'content-type': 'application/vnd.kafka.json.v2+json'}

    # read the raw transaction data
    transactions_df = read_from_csv(loc, start, end)
    NUM_TX = len(transactions_df)

    batch = []

    for index, row in transactions_df.iterrows():
        batch.append(row)

        if len(batch) % batch_size == 0:
            payload = {"records": []}

            for r in batch:
                record = {'value': r.to_json()}
                payload['records'].append(record)
            
            # post the payload with backoff/retry in case the bridge gets overloaded ...
            try:
                success = False
                retry = 0

                while not success:
                    r = requests.post(
                        KAFKA_ENDPOINT, headers=KAFKA_HEADERS, json=payload)
                    if r.status_code == 200:
                        success = True
                    else:
                        retry = retry + 1
                        if retry > 5:
                            print('aborting...')
                            sys.exit()
                        time.sleep(retry * 2)
                        print(f"backing-off/retry {retry}/5")
            except:
                print('exception/aborting...')
                sys.exit()

            batch = []
            print(f" --> uploaded {index+1}/{NUM_TX}")


def post_transactions(endpoint, start='2020-05-01', end='2020-05-01', loc='./data/archive/audit/', batch_size=1):
    files = []
    if loc.endswith('.csv'):
        files = [DIR_INPUT]
    else:
        # load all training files generated by the simulator
        files = [os.path.join(loc, f) for f in os.listdir(loc)]

    # read the raw transaction data
    transactions_df = load_transactions(files)

    for index, row in transactions_df.iterrows():
        print(row.to_json())
        sys.exit()
