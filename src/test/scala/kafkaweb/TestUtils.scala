package kafkaweb

import scala.concurrent.duration._
import scala.util.Random

trait TestUtils {
  val longAwait = 1.minutes

  def arbitraryString = Math.abs(Random.nextInt()).toString
}
