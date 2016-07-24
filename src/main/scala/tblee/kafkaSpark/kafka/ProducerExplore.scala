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
    val topic = s"test_topic"

    val testStr: String = s"Hello World!!"
    val kryoItem: Array[Byte] = KryoInjection(testStr)
    val kryoDecode = KryoInjection.invert(kryoItem).get

    println(kryoItem)
    println(kryoDecode.toString)

    val producerSink = sc.broadcast(KafkaProducerSink("localhost:9092", Option("test_topic")))

    val data = sc.parallelize(List(1, 2, 3, 4, 5))
    data.foreach{ value =>
      producerSink.value.send(value.toString, topic)
    }
  }
}
