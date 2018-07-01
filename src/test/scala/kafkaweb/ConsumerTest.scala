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

  "Consumer" should {
    "create consumer from start" in {
      val scenery = prepare(arbitraryString, 1000)
      val result = fetchWithConsumer(scenery.topic, None, scenery.keys.size)
      val keys = Await.result(result, longAwait)
      assert(keys.sorted == scenery.keys.map(_.key).sorted)
    }

    "create consumer with half offset" in {
      val scenery = prepare(arbitraryString, 1000)
      val halfAmount = scenery.keys.size / 2
      val middle = scenery.keys(halfAmount)
      val start = Map(0 -> middle.offset)
      val result = fetchWithConsumer(scenery.topic, Some(Consumer.Offsets(start)), halfAmount)
      val keys = Await.result(result, longAwait)
      assert(keys.sorted == scenery.keys.drop(halfAmount).map(_.key).sorted)
    }
  }

  override protected def afterAll(): Unit = {
    actorSystem.terminate()
  }

  private def fetchWithConsumer(topic: String, start: Option[Consumer.Offsets], expectedAmount: Int) = {
    Consumer.create(topic).flatMap { consumer =>
      def fetchNext(buffer: Vector[String], token: Option[Consumer.Offsets]): Future[Seq[String]] = {
        consumer.next(token)
          .flatMap { result =>
//            result.messages.foreach { m =>
//              println(s"consume ${m.key}")
//            }

            if (buffer.size >= expectedAmount) Future.successful(buffer)
            else fetchNext(buffer ++ result.messages.map(_.key), Some(result.offsets))
          }
      }

      fetchNext(buffer = Vector(), token = start)
    }
  }
}
