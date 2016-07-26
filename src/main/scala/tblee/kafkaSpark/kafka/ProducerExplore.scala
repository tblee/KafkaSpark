package tblee.kafkaSpark.kafka

import org.apache.spark.{SparkConf, SparkContext}
import com.twitter.chill.KryoInjection

import scala.util.Try


/**
  * Created by tblee on 7/23/16.
  */
object ProducerExplore {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ProducerExplore")
    val sc = new SparkContext(conf)

    val brokers = s"localhost:9092"
    val zkConnect = s"localhost:2181"
    val topic = s"test_topic2"

    val testStr: String = s"Hello World!!"
    val kryoItem: Array[Byte] = KryoInjection(testStr)
    val kryoDecode = KryoInjection.invert(kryoItem).get

    println(kryoItem)
    println(kryoDecode.toString)

    val producerSink = sc.broadcast(KafkaProducerSink[String]("localhost:9092", Option(topic)))

    val data = sc.parallelize(1 to 100)
    data.foreach{ value =>
      producerSink.value.send(value.toString, topic)
    }

    val consumerPool = new KafkaConsumer(topic, zkConnect, 1)
    consumerPool.startConsumers()
  }
}
