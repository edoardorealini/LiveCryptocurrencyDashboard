sh -c ' zookeeper-server-start.sh $KAFKA_HOME/config/zookeeper.properties & echo "Waiting zookeper" | sleep 3 &
        $KAFKA_HOME/bin/kafka-server-start.sh $KAFKA_HOME/config/server.properties &
        cassandra -f &
        python3 /cassandra/setup_cassandra.py & echo "Waiting cassandra setup" | sleep 10 &
        (cd /cryptoproducer; sbt run) &
        python3 /cryptoconsumer/LaunchCryptoConsumers.py &
        python3 dashapp.py
'