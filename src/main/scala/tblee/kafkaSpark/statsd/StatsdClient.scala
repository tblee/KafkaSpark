package tblee.kafkaSpark.statsd

import java.util.Random

/**
  * Created by tblee on 7/26/16.
  */

object StatsdClient extends Serializable{

  // Initialize with provided configurations
  private val host = "localhost"
  private val port = "8125"
  private val statPrefix = "scalaTest"

  // Predefined Statsd parameters
  // Suffix for increment stats.
  private val incrementSuffix = "c"
  // Suffix for timing stats.
  private val timingSuffix = "ms"
  // Suffix for gauge stats.
  private val gaugeSuffix = "g"

  private val r = new Random
  private def nextFloat: Float = r.nextFloat

  private def now: Long = System.currentTimeMillis


  private lazy val sender = new DatagramSender(host = host, port = port)

  def increment(key: String, value: Long = 1, samplingRate: Double = 1.0): Unit =
    send(key, value, incrementSuffix, samplingRate)

  def timing(key: String, millis: Long, samplingRate: Double = 1.0): Unit =
    send(key, millis, timingSuffix, samplingRate)

  def time[T](key: String, samplingRate: Double = 1.0)(operation: => T): T = {
    val start = now
    val timed = operation
    val end = now
    timing(key, end - start, samplingRate)
    timed
  }

  def gauge(key: String, value: Long): Unit =
    send(key, value, gaugeSuffix, 1.0)


  private def toStatsdMessage(key: String, value: Long, suffix: String, samplingRate: Double): String = {
    samplingRate match {
      case x if x >= 1.0 => s"${statPrefix}.${key}:${value}|${suffix}"
      case _ => s"${statPrefix}.${key}:${value}|${suffix}|@${samplingRate}"
    }
  }

  private def send(key: String, value: Long, suffix: String, samplingRate: Double): Unit = {
    val message = toStatsdMessage(key, value, suffix, samplingRate)
    if (samplingRate >= 1.0 || nextFloat < samplingRate) sender.send(message)
  }


}
