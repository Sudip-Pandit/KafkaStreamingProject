package KafkaStreamingPKG
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.jdbc._
import org.apache.spark.streaming._
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
object KafkaStreamingObject {

  def main(args: Array[String]): Unit={

    val conf = new SparkConf().setAppName("Kafka_streaming_demo_code").setMaster("local[*]")
      .set("spark.driver.allowMultipleContexts","true")

    val sc = new SparkContext(conf)

    sc.setLogLevel("Error")

    val spark = SparkSession
      .builder()
      .getOrCreate()

    import spark.implicits._

    val ssc = new StreamingContext(conf,Seconds(20))

    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "localhost:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> "example",
      "auto.offset.reset" -> "latest",
      "enable.auto.commit" -> (false: java.lang.Boolean))

    val topics = Array("Demo123")


    val stream = KafkaUtils.createDirectStream[String, String](
      ssc,
      PreferConsistent,Subscribe[String, String]
        (
          topics,
          kafkaParams)
    )

    val streamdata=		stream.map(record => (record.value))
    streamdata.print()

    ssc.start()
    ssc.awaitTermination()



  }




}
