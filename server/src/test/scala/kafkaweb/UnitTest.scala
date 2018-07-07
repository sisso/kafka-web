package kafkaweb

import akka.actor.ActorSystem
import kafkaweb.utils.IoExecutionContext
import org.scalatest.{BeforeAndAfterAll, WordSpecLike}

trait UnitTest extends WordSpecLike with TestUtils with BeforeAndAfterAll {
  implicit val actorSystem = ActorSystem()
  implicit val ex = actorSystem.dispatcher
  implicit val ioExecution = new IoExecutionContext(actorSystem.dispatchers.lookup("blocking-io-dispatcher"))

  override protected def afterAll(): Unit = {
    actorSystem.terminate()
  }
}
