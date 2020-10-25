package cryptoproducer

import scalaj.http.{Http, HttpOptions}
import spray.json._

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, ProducerConfig}
import kafka.producer.KeyedMessage
import java.util.Properties


case class Record(data: Map[String, Option[String]], timestamp: Long)

object MyJsonProtocol extends DefaultJsonProtocol {
  implicit val recordFormat = jsonFormat2(Record)
}

import MyJsonProtocol._

object CryptoProducer extends App{

  var wait_sec = 1

  //this function returns the current timestamp as Long and the current price as Double
  //given the name of the crypto from the api
  def getCurrentPrice(crypto_name : String): (String, String) = {       

    val response = Http("https://api.coincap.io/v2/assets/" + crypto_name).asString
    val response_body = response.body

    val parsed = JsonParser(response_body)
    val converted = parsed.convertTo[Record]

    //here we have to return a tuple timestamp, price
    return (converted.timestamp.toString, converted.data("priceUsd").toString.replace("Some(", "").replace(")", ""))
    
  }

  val brokers = "localhost:9092"
  val props = new Properties()
  props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers)
  props.put(ProducerConfig.CLIENT_ID_CONFIG, "CryptoProducer")
  props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
  props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

  val producer = new KafkaProducer[String, String](props)

  val cryptos: List[String] = List("bitcoin", "ethereum", "tether", "xrp", "litecoin", "cardano", "iota", "eos", "stellar")    
  //val cryptos: List[String] = List("bitcoin") 

  var count = 0   

  while(true){

    for(crypto <- cryptos){

      try{
        val time_price = getCurrentPrice(crypto)
        count = count + 1;
        val tuple = time_price._1 + "," + time_price._2
        println(crypto + ": \t" + tuple)
      
        val data = new ProducerRecord[String, String](crypto, null, tuple)
        producer.send(data)

        Thread.sleep((wait_sec*1000).toLong)
      }
      catch{
        case error: Throwable => {
          //error.printStackTrace
          print("Number of calls: ")
          print(count)
          println("[ERROR MANAGER] API calls error, waiting and slowly restarting.")
          println("[ERROR MANAGER] Waiting 20 seconds")
          Thread.sleep((20*1000).toLong)
          println("[ERROR MANAGER] Increasing the waiting time between calls.")
          wait_sec = wait_sec + 1
        }
      }    

    }

  }

  producer.close()

}