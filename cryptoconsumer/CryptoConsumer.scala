package cryptoconsumer

import java.util.HashMap
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka._
import kafka.serializer.{DefaultDecoder, StringDecoder}
import org.apache.spark.SparkConf
import org.apache.spark.streaming._
import org.apache.spark.streaming.kafka._
import org.apache.spark.storage.StorageLevel
import java.util.{Date, Properties}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import scala.util.Random

import org.apache.spark.sql.cassandra._
import com.datastax.spark.connector._
import com.datastax.driver.core.{Session, Cluster, Host, Metadata}
import com.datastax.spark.connector.streaming._

object CryptoConsumer {
  def main(args: Array[String]) {
    // connect to Cassandra and make a keyspace and table as explained in the document
    val cluster = Cluster.builder().addContactPoint("127.0.0.1").build()
    val session = cluster.connect()

    //session.execute("DROP KEYSPACE avg_space;")
    session.execute("CREATE KEYSPACE IF NOT EXISTS avg_space WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1};")
	  session.execute("CREATE TABLE IF NOT EXISTS avg_space.avg (word text PRIMARY KEY, count float);")

    //Spark straming configuration
    val conf = new SparkConf().setAppName("KafkaAverage").setMaster("local[*]")

    //Spark streaming context, microbatches every 1 second
    val ssc = new StreamingContext(conf, Seconds(1))
    //providing checkpoint directory to use stateful streams
    ssc.checkpoint("file:///tmp/spark/checkpoint")

    //Making a connection to Kafka and reading (key, value) pairs from it
    val kafkaConf = Map("metadata.broker.list" -> "localhost:9092",
                        "zookeeper.connect" -> "localhost:2181",
                        "group.id" -> "kafka-spark-streaming",
                        "zookeeper.connection.timeout.ms" -> "1000")

    val topic_set = Set("avg")

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](ssc, kafkaConf, topic_set)
    
    val touples = messages.map(el => (el._2).split(",")).map(el =>(el(0), el(1).toDouble))

    //log print to see if working
    //touples.foreachRDD(rdd => rdd.foreach(println))

    // measure the average value for each key in a stateful manner
    def mappingFunc(key: String, value: Option[Double], state: State[(Double, Int)]): (String, Double) = {
	    val (old_avg, old_count) = state.getOption.getOrElse(0.0, 0) 

      //Calculating the average through state
      //The state must keep into account the old average (old_avg) and how many occurencies happend (old_count)
      val new_count = old_count + 1
      val old_sum = old_count * old_avg

      val new_sum = old_sum + value.getOrElse(0.0)
      val new_avg = new_sum / new_count
      
      state.update((new_avg, new_count))
      
      return (key, new_avg)
    }

    val stateDstream = touples.mapWithState(StateSpec.function(mappingFunc _))

    // store the result in Cassandra
    stateDstream.saveToCassandra("avg_space", "avg", SomeColumns("word", "count"))

    ssc.start()
    ssc.awaitTermination()
  }
}
