package kafkaweb

import akka.Done
import akka.actor.ActorSystem
import kafkaweb.utils.IoExecutionContext
import org.scalatest.{BeforeAndAfterAll, WordSpec}

import scala.concurrent.{Await, Future}

class ConsumerTest extends WordSpec with ProducerScenery with BeforeAndAfterAll{
  implicit val actorSystem = ActorSystem()
  implicit val ex = actorSystem.dispatcher
  implicit val ioExecution = new IoExecutionContext(actorSystem.dispatchers.lookup("blocking-io-dispatcher"))

  val scenery = prepare("topic-2", 1000)

  "Consumer" should {
    "consume all messages from start if no offset is informed" in {
      val result = fetchWithConsumer(None)
      val keys = Await.result(result, longAwait)
      assert(keys.sorted == scenery.keys.map(_.key).sorted)
    }

    "consume half messages from middle" in {
      val middle = scenery.keys(scenery.keys.size / 2)
      val start = Map(0 -> middle.offset)
      val result = fetchWithConsumer(Some(Consumer.Offsets(start)))
      val keys = Await.result(result, longAwait)
      assert(keys.sorted == scenery.keys.map(_.key).sorted)
    }
  }

  override protected def afterAll(): Unit = {
    actorSystem.terminate()
  }

  private def fetchWithConsumer(start: Option[Consumer.Offsets]) = {
    Consumer.create("topic-0").flatMap { consumer =>
      def fetchNext(buffer: Vector[String], token: Option[Consumer.Offsets]): Future[Seq[String]] = {
        consumer.next(token)
          .flatMap { result =>
            if (buffer.size >= scenery.keys.size) Future.successful(buffer)
            else fetchNext(buffer ++ result.messages.map(_.key), Some(result.offsets))
          }
      }

      fetchNext(buffer = Vector(), token = start)
    }
  }
}
