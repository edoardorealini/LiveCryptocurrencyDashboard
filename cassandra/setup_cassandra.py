from cassandra.cqlengine import columns
from cassandra import ConsistencyLevel
from cassandra.cqlengine.models import Model
from cassandra.io.libevreactor import LibevConnection
from cassandra.cluster import Cluster
from cassandra.cqlengine.management import sync_table
from cassandra.query import SimpleStatement


def main():
    KEYSPACE = "cryptos_keyspace"

    currency = "bitcoin"

    cluster = Cluster()
    cluster.connection_class = LibevConnection
    session = cluster.connect()

    session.execute("DROP KEYSPACE " + KEYSPACE)

    session.execute("""
            CREATE KEYSPACE IF NOT EXISTS %s
            WITH replication = { 'class': 'SimpleStrategy', 'replication_factor': '2' }
            """ % KEYSPACE)

    session.set_keyspace(KEYSPACE)

    session.execute("CREATE TABLE IF NOT EXISTS " + currency + """
            (
                ts text,
                price text,
                date text,
                hour text,
                future text,
                PRIMARY KEY (ts)
            )
            """)


    prepared = session.prepare("INSERT INTO " + currency + """ (ts, price, date, hour, future)
        VALUES (?, ?, ?, ?, ?)
        """)

    for i in range(10):
        # session.execute(query, dict(key="ts%d" % i, price='price', date='date', hour=""))
        session.execute(prepared, ("ts%d" % i, '50.39', '2019-04-25', '00:00:00', '?'))

    future = session.execute_async("SELECT * FROM " + currency)

    try:
        rows = future.result()
    except Exception:
        print("Error reading rows")
        return

    for row in rows:
        print('\t'.join(row))

if __name__ == "__main__":
    main()
