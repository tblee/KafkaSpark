package tblee.kafkaSpark.kafka

import java.util.Properties
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}
import com.twitter.chill.KryoInjection

/**
  * Created by tblee on 7/23/16.
  */
class KafkaProducerSink[T](createProducer: () => KafkaProducer) extends Serializable {

  lazy val kafkaProducer = createProducer()
  lazy val kryoInjector = KryoInjection

  def send(value: T, topic: String): Unit = {
    val valueToSend = kryoInjector(value)
    kafkaProducer.send(valueToSend, topic)}
}

object KafkaProducerSink {
  def apply[T](brokerList: String,
               defaultTopic: Option[String]
              ): KafkaProducerSink[T] = {
    val f = () => {
      val producer = new KafkaProducer(brokerList, defaultTopic)
      sys.addShutdownHook(producer.close)
      producer
    }
    new KafkaProducerSink[T](f)
  }
}

case class KafkaProducer(brokerList: String,
                         defaultTopic: Option[String],
                         config: Properties = new Properties) {

  type Key = Array[Byte]
  type Val = Array[Byte]

  // Configuration and generate a Kafka producer instance
  private val producer = {
    val producerConfig = {
      val basicConfig = new Properties
      basicConfig.load(this.getClass.getResourceAsStream("../resources/producer-default.properties"))
      basicConfig.putAll(config)
      basicConfig.put("metadata.broker.list", brokerList)
      basicConfig
    }
    new Producer[Key, Val](new ProducerConfig(producerConfig))
  }

  def toMessage(value: Val, key: Option[Key], topicOpt: Option[String]): KeyedMessage[Key, Val] = {
    // Resolve topic
    val topic = topicOpt
      .getOrElse(defaultTopic
        .getOrElse(throw new IllegalArgumentException(s"Topic must be provided!")))

    // Packed message to emit
    key match {
      case Some(k) => new KeyedMessage(topic, k, value)
      case _ => new KeyedMessage(topic, value)
    }
  }

  def send(value: Val, key: Option[Key], topic: Option[String]): Unit = {
    producer.send(toMessage(value, key, topic))
  }

  def send(value: Val, topic: String): Unit = {
    send(value, None, Option(topic))
  }

  def close: Unit = producer.close
}
