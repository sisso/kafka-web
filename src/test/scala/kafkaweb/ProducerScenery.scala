package kafkaweb

import java.util.{Properties => JProperties}

import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.concurrent.{Await, ExecutionContext, Future, Promise}

object ProducerScenery {
  case class TestMessage(key: String, offset: Long)
  case class TestScenery(topic: String, keys: Seq[TestMessage])
}

trait ProducerScenery extends TestUtils {
  import ProducerScenery._

  def prepare(topic: String, numKeys: Int)(implicit executionContext: ExecutionContext): TestScenery =  {
    val props = new JProperties()
    props.put("bootstrap.servers", "localhost:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[String, String](props)

    val keys = (0 until numKeys).map(_.toString)

    val futures =
      keys.map { i =>
        val promise = Promise[TestMessage]()
        producer.send(new ProducerRecord[String, String](topic, i, i), new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit =
            if (Option(exception).isDefined)
              promise.failure(exception)
            else
              promise.success {
                TestMessage(
                  key = i,
                  offset = metadata.offset()
                )
              }
        })
        promise.future
      }

    val messages = Await.result(Future.sequence(futures), longAwait)

    producer.close()

    TestScenery(topic, messages)
  }
}
