package tblee.kafkaSpark.kafka

import java.util.Properties

import com.twitter.chill.KryoInjection
import kafka.producer.{KeyedMessage, Producer, ProducerConfig}

/**
  * Created by tblee on 7/23/16.
  */
class KafkaProducerSink(createProducer: () => KafkaProducer) extends Serializable {

  lazy val kafkaProducer = createProducer()
  lazy val kryoInjector = KryoInjection

  def send(value: Any, topic: String): Unit = {
    val valueToSend = kryoInjector(value)
    kafkaProducer.send(valueToSend, topic)}
}

object KafkaProducerSink {
  def apply(brokerList: String,
            defaultTopic: Option[String]
           ): KafkaProducerSink = {
    val f = () => {
      val producer = new KafkaProducer(brokerList, defaultTopic)
      sys.addShutdownHook(producer.close)
      producer
    }
    new KafkaProducerSink(f)
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
