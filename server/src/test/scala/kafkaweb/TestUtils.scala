package kafkaweb

import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

trait TestUtils {
  val shortAwait = 10.seconds

  val longAwait = 1.minutes

  def arbitraryString = Math.abs(Random.nextInt()).toString

  def await[T](f: Future[T]): T =
    Await.result(f, shortAwait)
}
