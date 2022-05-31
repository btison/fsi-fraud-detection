import os
import datetime
import pandas as pd

from kafka import KafkaConsumer, KafkaProducer
from json import loads, dumps

import warnings
warnings.filterwarnings("ignore")

# KAFKA server config
KAFKA_SVC = os.getenv('kafka_service')
KAFKA_SVC_PORT = os.getenv('kafka_service_port')
KAFKA_SERVER = f"{KAFKA_SVC}:{KAFKA_SVC_PORT}"

# service config
SOURCE_TOPIC = os.getenv('source_topic')
TARGET_TOPIC = os.getenv('target_topic')

CLIENT_ID = os.getenv('client_id')
GROUP_ID = os.getenv('group_id')

tx_history = pd.DataFrame(columns=['TRANSACTION_ID',
                                   'TX_DATETIME',
                                   'CUSTOMER_ID',
                                   'TERMINAL_ID',
                                   'TX_AMOUNT',
                                   'TX_TIME_SECONDS',
                                   'TX_TIME_DAYS'])


def is_weekend(tx_datetime):

    # Transform date into weekday (0 is Monday, 6 is Sunday)
    weekday = tx_datetime.weekday()
    # Binary value: 0 if weekday, 1 if weekend
    is_weekend = weekday >= 5

    return int(is_weekend)


def is_night(tx_datetime):

    # Get the hour of the transaction
    tx_hour = tx_datetime.hour
    # Binary value: 1 if hour less than 6, and 0 otherwise
    is_night = tx_hour <= 6

    return int(is_night)


def get_customer_spending_behaviour_features(customer_transactions, windows_size_in_days=[1, 7, 30]):

    # Let us first order transactions chronologically
    customer_transactions = customer_transactions.sort_values('TX_DATETIME')

    # The transaction date and time is set as the index, which will allow the use of the rolling function
    customer_transactions.index = customer_transactions.TX_DATETIME

    # For each window size
    for window_size in windows_size_in_days:

        # Compute the sum of the transaction amounts and the number of transactions for the given window size
        SUM_AMOUNT_TX_WINDOW = customer_transactions['TX_AMOUNT'].rolling(
            str(window_size)+'d').sum()
        NB_TX_WINDOW = customer_transactions['TX_AMOUNT'].rolling(
            str(window_size)+'d').count()

        # Compute the average transaction amount for the given window size
        # NB_TX_WINDOW is always >0 since current transaction is always included
        AVG_AMOUNT_TX_WINDOW = SUM_AMOUNT_TX_WINDOW/NB_TX_WINDOW

        # Save feature values
        customer_transactions['CUSTOMER_ID_NB_TX_' +
                              str(window_size)+'DAY_WINDOW'] = list(NB_TX_WINDOW)
        customer_transactions['CUSTOMER_ID_AVG_AMOUNT_' +
                              str(window_size)+'DAY_WINDOW'] = list(AVG_AMOUNT_TX_WINDOW)

    # Reindex according to transaction IDs
    customer_transactions.index = customer_transactions.TRANSACTION_ID

    # And return the dataframe with the new features
    return customer_transactions


def prepare(tx):
    #tx_dt = datetime.datetime.fromisoformat(tx['TX_DATETIME'])
    tx_dt = datetime.datetime.fromtimestamp(tx['TX_DATETIME']/1000)
    tx['TX_DATETIME'] = tx_dt
    tx['TX_DURING_WEEKEND'] = is_weekend(tx_dt)
    tx['TX_DURING_NIGHT'] = is_night(tx_dt)

    return tx


def transform(tx):
    # convert from Timestamp to UTC seconds, otherwise json pukes
    dt = tx['TX_DATETIME']
    utc_time = dt.replace(tzinfo=datetime.timezone.utc)
    tx['TX_DATETIME'] = int(utc_time.timestamp())

    # TX_FRAUD,TX_FRAUD_SCENARIO -> -1 i.e. we don't know ...
    tx['TX_FRAUD'] = -1
    tx['TX_FRAUD_SCENARIO'] = -1

    # Keep it out of the tx data as since we can't calculate it at this point in time
    # The terminal risk can only be calculated once the fraud assessment has taken place.
    tx['TERMINAL_ID_NB_TX_1DAY_WINDOW'] = 0
    tx['TERMINAL_ID_RISK_1DAY_WINDOW'] = 0
    tx['TERMINAL_ID_NB_TX_7DAY_WINDOW'] = 0
    tx['TERMINAL_ID_RISK_7DAY_WINDOW'] = 0
    tx['TERMINAL_ID_NB_TX_30DAY_WINDOW'] = 0
    tx['TERMINAL_ID_RISK_30DAY_WINDOW'] = 0

    return tx


consumer = KafkaConsumer(
    SOURCE_TOPIC,
    bootstrap_servers=[KAFKA_SERVER],
    auto_offset_reset='earliest',
    consumer_timeout_ms=-1,
    enable_auto_commit=True,
    client_id=CLIENT_ID,
    group_id=GROUP_ID,
    value_deserializer=lambda x: loads(x.decode('utf-8')))

producer = KafkaProducer(bootstrap_servers=[
                         KAFKA_SERVER], value_serializer=lambda x: dumps(x).encode('utf-8'))

print(f" --> listening on topic '{SOURCE_TOPIC}'")

for msg in consumer:
    # no idea, why we need the doubble loads ...
    #tx = loads(msg.value)
    tx = msg.value

    # add the new tx to the in-memory 'database'
    tx_history = tx_history.append(prepare(tx), ignore_index=True)

    # calculate the spending behaviour in 1,7,30 time window
    tx_spending = get_customer_spending_behaviour_features(
        tx_history[tx_history.CUSTOMER_ID == tx['CUSTOMER_ID']])

    # only the latest tx is relevant
    tx0 = tx_spending[tx_spending.TRANSACTION_ID == tx['TRANSACTION_ID']]

    # transform the data
    tx1 = transform(tx0.to_dict(orient='records')[0])

    # send it to the tx topic
    producer.send(TARGET_TOPIC, value=tx1)

    # basic logging, because demo
    print(
        f" --> {tx['TRANSACTION_ID']}:[{tx['TX_DATETIME']},{tx['TERMINAL_ID']},{tx['TX_AMOUNT']}]")
