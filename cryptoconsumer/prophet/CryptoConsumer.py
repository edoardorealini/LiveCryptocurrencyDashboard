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

def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, colnames)

def main():
    cryptos = ["bitcoin", "ethereum"]
    plot = False #Triggers the plotting of the results, only for test purposes

    #Initiating connection to Cassandra and using the keyspace cryptos_keyspace
    cluster = Cluster(["127.0.0.1"], port=9042)
    session = cluster.connect('cryptos_keyspace',wait_for_all_pools=True)
    session.execute("USE cryptos_keyspace")
    #session.row_factory = pandas_factory
    #session.default_fetch_size = None

    for crypto in cryptos:
        #Data loading phase - SUBSTITUTE WITH CASSANDRA
        #data = pd.read_csv("../../data/history/" + str(crypto) + "/" + "only_days_" + str(crypto) + ".csv")
        query = 'SELECT * FROM ' + crypto
        query_result = session.execute(query, timeout=None)
        #data = query_result._current_rows
        data = pd.DataFrame(list(query_result))

        #Data preprocessing
        data = data[["date", "price"]]
        data = data.rename(columns = {"date": "ds", "price": "y"})
        data = data.sort_values(by="ds")
        #print(data.tail(10))

        #Creating and fitting the Prophet model
        prophet = Prophet(daily_seasonality = True)
        prophet.fit(data)

        #Generating a dataframe containing the predictions of price fluctuation for the followin 7 days
        future = prophet.make_future_dataframe(periods=7)
        predictions = prophet.predict(future)

        #predictions.to_csv("./predictions/trial.csv")

        if plot:
            #Plotting the results with plt
            prophet.plot(predictions)

            plt.title("Predictions for Bitcoin price in the next 7 days")
            plt.xlabel("Date")
            plt.ylabel("Price USD")
            plt.show()
            

        consumer = KafkaConsumer(
            crypto,
            bootstrap_servers=["localhost:9092"],
            auto_offset_reset="latest",
            enable_auto_commit=True,
            group_id="dataintensive",
            value_deserializer=lambda x: str(x).replace("\'", "").replace("b", "").split(",")
        )

        for message in consumer:
            print(timestamp_to_date(message.value[0]), message.value[1])
            row = [timestamp_to_date(message.value[0]), message.value[1]]
            new_row = pd.DataFrame([row], columns=["ds", "y"])
            data = data.append(new_row, ignore_index=False)

            print(data.tail(10))

            #print(data.tail(10))



if __name__ == "__main__":
    main()