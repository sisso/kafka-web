package kafkaweb

import scala.concurrent.{Await, Future}

class ConsumerTest extends UnitTest with ProducerScenery {
  "Consumer" should {
    "create consumer from start" in {
      val scenery = prepare(arbitraryString, 1000)
      val result = fetchWithConsumer(scenery.topic, None, scenery.keys.size)
      val keys = Await.result(result, shortAwait)
      assert(keys.sorted == scenery.keys.map(_.key).sorted)
    }

    "create consumer with half offset" in {
      val scenery = prepare(arbitraryString, 1000)
      val halfAmount = scenery.keys.size / 2
      val middle = scenery.keys(halfAmount)
      assert(middle.key == "500")
      val start = Map(0 -> middle.offset)
      val result = fetchWithConsumer(scenery.topic, Some(Consumer.Offsets(start)), halfAmount - 1)
      val keys = Await.result(result, shortAwait)
      assert(keys.head == "501")
      assert(keys.sorted == scenery.keys.drop(halfAmount + 1).map(_.key).sorted)
    }

    "support parallel retries during consumer" in {
      val scenery = prepare("topic-0", 10000)
      val consumer = Await.result(Consumer.create(scenery.topic), shortAwait)

      val next = Await.result(consumer.next(None), shortAwait).offsets

      await {
        val f1 = consumer.next(Some(next))
        val f2 = consumer.next(Some(next))

        for {
          r1 <- f1
          r2 <- f2
        } yield {
          assert(r1.messages.nonEmpty)
          assert(r1 == r2)
        }
      }
    }
  }

  private def fetchWithConsumer(topic: String, start: Option[Consumer.Offsets], expectedAmount: Int) = {
    Consumer.create(topic).flatMap { consumer =>
      def fetchNext(buffer: Vector[String], token: Option[Consumer.Offsets]): Future[Seq[String]] = {
        consumer.next(token)
          .flatMap { result =>
            if (buffer.size >= expectedAmount) Future.successful(buffer)
            else fetchNext(buffer ++ result.messages.map(_.key), Some(result.offsets))
          }
      }

      fetchNext(buffer = Vector(), token = start)
    }
  }
}
