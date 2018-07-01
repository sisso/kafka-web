package kafkaweb

import java.util.{Arrays => JArrays, Properties => JProperties}

import akka.Done
import akka.actor.ActorSystem
import akka.event.Logging
import kafkaweb.utils.IoExecutionContext
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.collection.immutable
import scala.concurrent.Future
import scala.util.Random

trait Consumer {
  def next(maybeOffsets: Option[Consumer.Offsets]): Future[Consumer.Result]

  def close(): Future[Done]
}

object Consumer {
  case class Offsets(value: Map[Int, Long])
  case class Message(key: String, value: String)
  case class Result(messages: Seq[Message], offsets: Offsets)

  def create(topic: String)(implicit actorSystem: ActorSystem, ioExecutionContext: IoExecutionContext): Future[Consumer] = Future.successful {
    new Consumer {
      private val lock = new Object()

      private var consumerHolder: Option[KafkaConsumer[String, String]] = None

      private var state = Offsets(Map.empty)

      private val log = Logging(actorSystem, s"Consumer-${topic}")

      override def next(maybeOffsets: Option[Consumer.Offsets]): Future[Result] = ioExecutionContext.block {
        lock.synchronized {
          val consumer =
            consumerHolder match {
              case Some(consumer) => consumer
              case None =>
                val props = new JProperties()
                props.put("bootstrap.servers", "localhost:9092")
                props.put("group.id", arbitraryString)
                props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
                props.put("auto.offset.reset", "earliest")
                props.put("enable.auto.commit", "false")

                val consumer = new KafkaConsumer[String, String](props)

                maybeOffsets match {
                  case Some(offsets) =>
                    log.info("creating new consumer {} at offsets {}", topic, offsets)

                    val assign =
                      offsets.value.map { case (partition, _) =>
                        new TopicPartition(topic, partition)
                      }.toSeq.asJava

                    consumer.assign(assign)

                    offsets.value.foreach { case (partition, offset) =>
                      log.debug("seek {} to {}/{}", topic, partition, offset)
                      consumer.seek(new TopicPartition(topic, partition), offset)
                    }

                    state = offsets

                  case None =>
                    log.info("creating new consumer {}", topic)
                    consumer.subscribe(JArrays.asList(topic))
                }

                consumerHolder = Some(consumer)

                consumer
            }

          maybeOffsets match {
            case Some(offsets) =>
              if (offsets == state) {
                log.info("valid state")
              } else {
//                  log.info("invalid state, seeking to {}", offsets)
//                  consumer
//                    .assignment()
//                    .asScala
//                    .toSeq
//                    .sortBy(_.partition())
//                    .zip(offsets)
//                    .foreach { case (partition, offset) =>
//                      log.info("seeking {} to {}", partition, offset)
//                      consumer.seek(partition, offset)
//                    }

                ???
              }

            case None =>
              log.info("seeking to start")
              consumer.seekToBeginning(consumer.assignment())
          }

          val records = consumer.poll(100)

          val recs = records
            .iterator()
            .asScala
            .toVector

          val messages = recs.map { record => Message(record.key(), record.value()) }

          val nextOffsets =
            Offsets {
              recs
              .groupBy(_.partition())
              .map { i =>
                i._1 -> i._2.map(_.offset()).max
              }
            }

          state = nextOffsets

          log.info("consuming {} messages and moving local offset to {}", messages.size, state)

          Result(messages ,nextOffsets)
        }
      }

      override def close(): Future[Done] = {
        ioExecutionContext.block {
          consumerHolder.foreach { _.wakeup() }

          lock.synchronized {
            consumerHolder.foreach(_.close())
          }

          Done
        }
      }
    }
  }

  private def arbitraryString = Math.abs(Random.nextInt()).toString
}
