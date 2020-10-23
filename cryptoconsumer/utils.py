import pandas as ps
from cassandra.cluster import Cluster
from cassandra.auth import PlainTextAuthProvider
from cassandra.query import dict_factory
from cassandra.cqlengine import columns
from cassandra import ConsistencyLevel
from cassandra.cqlengine.models import Model
from cassandra.io.libevreactor import LibevConnection
from cassandra.query import SimpleStatement
import pandas as pd
import time
import requests as req


def pandas_factory(colnames, rows):
    return pd.DataFrame(rows, columns=colnames)

def get_updated_df(currency):
    """
    This function gets in INPUT the name of the crypto the consumer is dealing with.
    It checks if the Cassandra DB is up to date w.r.t. to the current day and eventually updates the DB.
    Then it RETURNS the updated DB as a Pandas DataFrame to the consumer.
    NB: the Cassandra DB will be updated until the day before today (YESTERDAY), it's duty of the consumer
        reading from Kafka the current price of the crypto.
    """

    KEYSPACE = "cryptos_keyspace"

    # opening cassandra to retrieve the current df
    cluster = Cluster()
    cluster.connection_class = LibevConnection
    session = cluster.connect()
    session.set_keyspace(KEYSPACE)

    session.row_factory = pandas_factory
    session.default_fetch_size = None

    query = "SELECT * FROM {}.{};".format(KEYSPACE, currency)

    result = session.execute(query, timeout=None)
    df = result._current_rows

    # sort the df by timestamp, so that the first row is the last entry
    df = df.sort_values(by=["ts"], ascending=False)

    # getting only the last information stored in Cassandra
    last_entry_cassandra = df.iloc[0]
    last_day = last_entry_cassandra["date"]
    last_ts = last_entry_cassandra["ts"]
    last_hour = last_entry_cassandra["hour"]

    index_last_entry = df[df["ts"] == last_ts].index.values

    #print(index_last_entry)
    #print(len(df.index))
    #print(df)

    timestamp_curr = int(time.time()) * 1000    # to get milliseconds

    if int(last_ts) < int(timestamp_curr):
        # now get the actual price of the currency from the API
        url = "https://api.coincap.io/v2/assets/" + currency + "/history?interval=d1"
        start = "&start=" + str(last_ts)
        end = "&end=" + str(timestamp_curr)
        final_url = url + start + end

        response = req.get(final_url).json()

        for data_point in response["data"]:
            timestamp = data_point["time"]
            date = data_point["date"].replace(".000Z", "")
            split_date = date.split("T")
            date = split_date[0]
            hour = split_date[1]
            priceUsd = data_point["priceUsd"]

            if int(timestamp) > int(last_ts):

                df.loc[len(df.index)] = [timestamp, date, hour, priceUsd, "?", "?", "?"]

                command = "INSERT INTO {} (ts, price, date, hour, yhat, yhat_lower, yhat_upper) VALUES (?, ?, ?, ?, ?, ?, ?)".format(currency)

                prepared = session.prepare(command)

                session.execute(prepared, ("{}".format(timestamp),
                                           "{}".format(priceUsd),
                                           "{}".format(date),
                                           "{}".format(hour),
                                           "?", "?", "?"))

    return df.tail(len(df.index) - 7)


# ---------------- MAIN ---------------

# newdf = update_cassandra("bitcoin")
# print(newdf)
