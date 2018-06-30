package kafkaweb

import java.util

import akka.Done
import org.apache.kafka.clients.consumer.ConsumerRecords

import scala.concurrent.Future

case class NextOffsets()

case class Result(records: ConsumerRecords[String, String])

trait Producer {
  def next(): Future[(Result, NextOffsets)]

  def close(): Future[Done]
}

object Producer {
  def create(topic: String): Future[Producer] = Future.successful {
    new Producer {
//      val consumer = new KafkaConsumer[String, String]()

      override def next(): Future[(Result, NextOffsets)] = Future.successful {
//        consumer.poll()
        (Result(new ConsumerRecords(new util.HashMap())), NextOffsets())
      }

      override def close(): Future[Done] = {
        Future.successful(Done)
      }
    }
  }
}