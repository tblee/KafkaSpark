package tblee.kafkaSpark.statsd

import java.net.{DatagramSocket, DatagramPacket, InetAddress}

/**
  * Created by tblee on 7/26/16.
  */
case class PacketData(value: Array[Byte]) extends AnyVal

case class Packet(data: PacketData, host: InetAddress, port: Int) {
  def getPacket: DatagramPacket =
    new DatagramPacket(data.value, data.value.length, host, port)
}

class DatagramSender(host: String,
                     port: String) extends Serializable {

  private lazy val socket: DatagramSocket = new DatagramSocket
  private val ht: InetAddress = InetAddress.getByName(host)
  private val pt: Int = port.toInt

  def send(message: String): Unit = {
    try {
      val bytes = PacketData(message.getBytes)
      val packet = Packet(bytes, ht, pt)
      socket.send(packet.getPacket)
    } catch {
      case error: Throwable => println(s"Error sending message!")
    }

  }
}

