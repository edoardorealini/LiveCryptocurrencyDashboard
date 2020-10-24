import threading
import time
import datetime
import json

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
from utils import get_updated_df

def timestamp_to_date(timestamp):
    timestamp = int(timestamp) / 1000
    return time.strftime("%Y-%m-%d", time.localtime(timestamp))


def date_to_timestamp(date):
    #print(date)
    return int(datetime.datetime.strptime(str(date).replace(" 00:00:00", ""), '%Y-%m-%d').strftime("%s")) * 1000

threadLock = threading.Lock()

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
            threadLock.acquire()

            data = get_updated_df(self.crypto)

            # Data preprocessing
            data = data[["date", "price"]]
            data = data.rename(columns = {"date": "ds", "price": "y"})
            data = data.sort_values(by="ds", ascending=True)

            # For each new message, update the dataset and retrain the whole model
            row = [timestamp_to_date(message.value[0]), message.value[1]]
            new_row = pd.DataFrame([row], columns=["ds", "y"])

            # Rick's function will also get the newest full dataframe, I will only need to append the last record and train the model.
            data = data.append(new_row, ignore_index=True)

            # This filter gives to the model only the last N records, This improves the performance of the system but increases the noise in the prediction.
            # Using only the last 90 records to make the predictions.
            data = data.tail(60)

            # Creating and fitting the Prophet model
            prophet = Prophet(daily_seasonality = True)
            prophet.fit(data)

            # Generating a dataframe containing the predictions of price fluctuation for the followin 7 days
            future = prophet.make_future_dataframe(periods=7)
            predictions = prophet.predict(future)

            predictions = predictions.tail(7)
            predictions = predictions[["ds", "yhat", "yhat_lower", "yhat_upper"]]

            # store in cassandra the date of today
            today = timestamp_to_date(message.value[0])
            cassandra_command_today = "INSERT INTO {} (ts, price, date, hour, yhat, yhat_lower, yhat_upper) VALUES (?, ?, ?, ?, ?, ?, ?)".format(self.crypto)
            prepared_today = session.prepare(cassandra_command_today)

            session.execute(prepared_today, ("{}".format(date_to_timestamp(today)),
                                            "{}".format(message.value[1]),
                                            today,
                                            "{}".format("00:00:00"),
                                            "?", "?", "?",
                                            )
                            )

            #print(predictions)
            # Putting the results back to Cassandra 
            # Putting results into Cassandra, in the future field of the interested days.
            cassandra_command = "INSERT INTO {} (ts, price, date, hour, yhat, yhat_lower, yhat_upper) VALUES (?, ?, ?, ?, ?, ?, ?)".format(self.crypto)
            prepared = session.prepare(cassandra_command)

            for i in range(len(predictions["ds"])):
                row = predictions.iloc[i]

                date = str(row["ds"]).replace(" 00:00:00", "")

                session.execute(prepared, ("{}".format(date_to_timestamp(row["ds"])),
                                        "{}".format(row["yhat"]),
                                        date,
                                        "{}".format("00:00:00"),
                                        "{}".format(row["yhat"]),
                                        "{}".format(row["yhat_lower"]),
                                        "{}".format(row["yhat_upper"]),
                                        )
                                )
            
            threadLock.release()
