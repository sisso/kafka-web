package kafkaweb

import java.util.concurrent.atomic.AtomicInteger
import java.util.{Properties => JProperties}

import akka.event.Logging
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

import scala.concurrent.duration._
class ProducerUtils extends UnitTest {
  "keep producing" in {
    val topic = "topic-3"
    val messageEach = 100.millis
    val totalTime = 2.hours
    val log = Logging(actorSystem, this.getClass)

    val props = new JProperties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)
    val counter = new AtomicInteger()

    actorSystem.scheduler.schedule(messageEach, messageEach) {
      val i = "%04d".format(counter.incrementAndGet())
      val meta = producer.send(new ProducerRecord[String, String](topic, i, i)).get()
      log.debug("publish {} at {}/{}", i, meta.partition(), meta.offset())
    }

    Thread.sleep(totalTime.toMillis)
    log.debug("closing")
    producer.close()
  }
}
