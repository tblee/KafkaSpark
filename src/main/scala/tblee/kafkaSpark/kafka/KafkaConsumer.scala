package tblee.kafkaSpark.kafka

import java.util.Properties
import java.util.concurrent.{ExecutorService, Executors}

import kafka.consumer.{Consumer, ConsumerConfig, KafkaStream}
import kafka.message.MessageAndMetadata
import com.twitter.chill.KryoInjection

/**
  * Created by tblee on 7/23/16.
  */
class KafkaConsumer(val topic: String,
                    val zkConnect: String,
                    numStreams: Int,
                    config: Properties = new Properties) {

  val consumerConfig = {
    val basicConfig = new Properties
    basicConfig.load(this.getClass.getResourceAsStream("../resources/consumer-default.properties"))
    basicConfig.putAll(config)
    basicConfig.put("zookeeper.connect", zkConnect)
    basicConfig
  }

  private val consumerPool: ExecutorService = Executors.newFixedThreadPool(numStreams)
  private val consumerConnect = Consumer.create(new ConsumerConfig(consumerConfig))

  def startConsumers(): Unit = {
    val topicCountMap = Map(topic -> numStreams)
    val messageStreams = consumerConnect.createMessageStreams(topicCountMap)
    val consumerThreads = messageStreams.get(topic) match {
      case Some(streams) => streams.view.zipWithIndex map { case (stream, tId) =>
        new KafkaConsumerTask(stream, tId)
      }
      case _ => Seq()
    }

    consumerThreads foreach consumerPool.submit
  }

  def shutdown(): Unit = {
    println(s"Shutting down Kafka consumer(s)...")
    consumerConnect.shutdown()
    println(s"Shutting down consumer thread pool...")
    consumerPool.shutdown()
  }

  Runtime.getRuntime().addShutdownHook(new Thread() {
    override def run(): Unit = {
      shutdown()
    }
  })

}

//case class ConsumerTaskContext(threadId: String, config: Properties)

class KafkaConsumerTask(dataStream: KafkaStream[Array[Byte], Array[Byte]],
                        threadNum: Int
                       ) extends Runnable {

  private val kryoDecoder = KryoInjection

  override def run(): Unit = {
    try {
      println(s"Starting thread-${threadNum}")
      dataStream foreach {
        case msg: MessageAndMetadata[_, _] => println(s"Received message - thread-${threadNum}:" +
          s" ${kryoDecoder.invert(msg.message).get}.")
        case _ => println(s"Received unexpected message!! - thread-${threadNum}")
      }
      println(s"Consumer thread shutting down - thread-${threadNum}...")
    } catch {
      case e: InterruptedException => println(s"Consumer task interrupted - thread-${threadNum}!")
    }
  }

}
