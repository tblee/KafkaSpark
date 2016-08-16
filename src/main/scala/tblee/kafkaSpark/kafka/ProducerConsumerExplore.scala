package tblee.kafkaSpark.kafka

import tblee.kafkaSpark.statsd.StatsdClient
import org.apache.spark.{SparkConf, SparkContext}


/**
  * Created by tblee on 7/23/16.
  */
object ProducerConsumerExplore {
  def main(args: Array[String]) {

    val conf = new SparkConf().setAppName("ProducerExplore")
    val sc = new SparkContext(conf)

    val brokers = s"localhost:9092"
    val zkConnect = s"localhost:2181"
    val topic = s"test_topic"

    // Broadcast producerSink, producers are lazily instantiated.
    val producerSink = sc.broadcast(KafkaProducerSink[String](brokers, Option(topic)))
    val data = sc.parallelize(1 to 100, 2)
    data.foreachPartition{ part =>
      part foreach { value =>
        producerSink.value.send(value.toString, topic)
        StatsdClient.increment("emitted")
      }
    }

    // Kick-start consumer pool
    val consumerPool = new KafkaConsumer(topic, zkConnect, 2)
    consumerPool.startConsumers()
  }
}
