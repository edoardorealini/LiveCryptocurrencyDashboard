from cassandra.cqlengine import columns
from cassandra import ConsistencyLevel
from cassandra.cqlengine.models import Model
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import SimpleStatement
import pandas as pd


def main():
    KEYSPACE = "cryptos_keyspace"

    assets = ['bitcoin', "ethereum", "tether", "xrp", "litecoin", "cardano", "iota", "eos", "stellar"]      # missing quel bastardo di bitcoin-cash

    cluster = Cluster()
    cluster.connection_class = LibevConnection
    session = cluster.connect()

    session.execute("DROP KEYSPACE " + KEYSPACE)

    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
            """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    for currency in assets:

        create_table_command = "CREATE TABLE IF NOT EXISTS {} (ts text, price text, date text, hour text, future text, PRIMARY KEY (ts))".format(currency)

        session.execute(create_table_command)

        command = "INSERT INTO {} (ts, price, date, hour, future) VALUES (?, ?, ?, ?, ?)".format(currency)

        prepared = session.prepare(command)

        folder_path = "../data/history/" + currency + "/only_days_" + currency + ".csv"

        df = pd.read_csv(folder_path)

        for i in range(len(df["timestamp"])):
            # session.execute(query, dict(key="ts%d" % i, price='price', date='date', hour=""))
            row = df.iloc[i]
            session.execute(prepared, ("{}".format(row["timestamp"]),
                                       "{}".format(row["price"]),
                                       "{}".format(row["date"]),
                                       "{}".format(row["hour"]),
                                       "?"))

if __name__ == "__main__":
    main()