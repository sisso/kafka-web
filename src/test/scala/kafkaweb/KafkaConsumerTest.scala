package kafkaweb

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.scalatest.WordSpec
import java.util.{Arrays => JArrays, Properties => JProperties}

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.internals.ConsumerProtocol
import org.apache.kafka.common.TopicPartition

import scala.collection.JavaConverters._
import scala.util.Random

class KafkaConsumerTest extends WordSpec {


  "direct consumer" should {
    "subscribe and seek" in {
      // val (topic, keys) = prepare()

      val topic = "topic-1"

      val props = new JProperties()
      props.put("bootstrap.servers", "localhost:9092")
      props.put("group.id", arbitraryString)
      props.put("enable.auto.commit", "false")
//      props.put("auto.commit.interval.ms", "1000")
      props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
      props.put("auto.offset.reset", "earliest")

      val consumer = new KafkaConsumer[String, String](props)
//      consumer.subscribe(JArrays.asList(topic))
      consumer.assign(JArrays.asList(new TopicPartition(topic, 0)))
      consumer.seek(new TopicPartition(topic, 0), 1990)

//      val partitions =
//        consumer
//          .partitionsFor(topic)
//          .asScala
//          .map { p =>
//            new TopicPartition(p.topic(), p.partition())
//          }

//      consumer.seekToEnd(null)

      while (true) {
        val records = consumer.poll(1000)

        records.iterator().asScala.foreach { msg =>
          println(msg.partition() + ":" + msg.offset() + "/" + msg.key())
        }
      }
    }
  }

  private def prepare(): (String, Seq[String]) = {
    val props = new JProperties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val topic = "topic-1"

    val keys = (0 until 1000).map(_.toString)

    val futures =
      keys.map { i =>
        producer.send(new ProducerRecord[String, String](topic, i, i))
      }

    futures.foreach(_.get())
    producer.close()

    (topic, keys)
  }

  private def arbitraryString = Math.abs(Random.nextInt()).toString
}
