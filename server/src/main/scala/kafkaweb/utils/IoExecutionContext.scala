package kafkaweb.utils

import scala.concurrent.{ExecutionContext, Future}

class IoExecutionContext(val ex: ExecutionContext) {
  def block[T](f: => T): Future[T] = Future {
    scala.concurrent.blocking {
      f
    }
  }(ex)
}
