import threading
import time
from datetime import datetime

import pandas as pd 
import numpy as np
import matplotlib.pyplot as plt

from cassandra.cqlengine import columns
from cassandra import ConsistencyLevel
from cassandra.cqlengine.models import Model
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import SimpleStatement

import time
from kafka import KafkaConsumer

from fbprophet import Prophet

def timestamp_to_date(timestamp):
    timestamp = int(timestamp) / 1000
    return time.strftime("%Y-%m-%d", time.localtime(timestamp))


class CryptoConsumerThread(threading.Thread):
    def __init__(self, crypto):
        threading.Thread.__init__(self)
        self.crypto = crypto

    def run(self):
        # Thread Code!

        # Initiating connection to Cassandra and using the keyspace cryptos_keyspace
        cluster = Cluster(["127.0.0.1"], port=9042)
        session = cluster.connect('cryptos_keyspace',wait_for_all_pools=True)
        session.execute("USE cryptos_keyspace")

        # Data loading phase
        query = 'SELECT * FROM ' + self.crypto
        query_result = session.execute(query, timeout=None)
        data = pd.DataFrame(list(query_result))

        # Data preprocessing
        data = data[["date", "price"]]
        data = data.rename(columns = {"date": "ds", "price": "y"})
        data = data.sort_values(by="ds")
      
        # Setup of Kafka connection
        consumer = KafkaConsumer(
            self.crypto,
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="dataintensive",
            value_deserializer=lambda x: str(x).replace("\'", "").replace("b", "").split(",")
        )

        for message in consumer:
            # For each new message, update the dataset and retrain the whole model
            row = [timestamp_to_date(message.value[0]), message.value[1]]
            new_row = pd.DataFrame([row], columns=["ds", "y"])

            #TODO Check if the data in cassandra is updated, otherwise update it (Rick's function)

            data = data.append(new_row, ignore_index=True)

            # Creating and fitting the Prophet model
            prophet = Prophet(daily_seasonality = True)
            prophet.fit(data)

            # Generating a dataframe containing the predictions of price fluctuation for the followin 7 days
            future = prophet.make_future_dataframe(periods=7)
            predictions = prophet.predict(future)

            predictions = predictions.tail(7)
            predictions = predictions[["ds", "yhat", "yhat_lower", "yhat_upper"]]

            print(predictions)
            # Putting the results back to Cassandra 
            # TODO put results into Cassandra, in the future field of the interested days.





'''
# The following code should be called when the threads are actually created
# Wait for all threads to complete
for t in threads:
    t.join()
print("Exiting Main Thread")
'''