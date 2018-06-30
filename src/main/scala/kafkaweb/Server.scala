package kafkaweb

import java.util.concurrent.{CompletableFuture, Executor, TimeUnit}

import akka.actor.ActorSystem
import akka.event.Logging
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.http.scaladsl.server.Directives._
import akka.stream.ActorMaterializer
import com.github.benmanes.caffeine.cache.{AsyncCacheLoader, Caffeine, RemovalCause, RemovalListener}
import org.json4s.DefaultFormats
import org.json4s.native.Serialization

import scala.collection.JavaConverters._
import scala.compat.java8.FutureConverters._
import scala.io.StdIn
import scala.util.{Failure, Success}

object Server {
  implicit val system = ActorSystem()
  implicit val materializer = ActorMaterializer()
  implicit val executionContext = system.dispatcher

	def main(args: Array[String]): Unit = {
    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher
    implicit val log = Logging(system, "server")
    implicit val formats = DefaultFormats

    val cache =
      Caffeine.newBuilder()
        .maximumSize(100)
        .expireAfterWrite(10, TimeUnit.MINUTES)
        .expireAfterAccess(1, TimeUnit.MINUTES)
        .executor(system.dispatcher)
        .removalListener(new RemovalListener[String, Producer] {
          override def onRemoval(key: String, value: Producer, cause: RemovalCause): Unit = {
            log.debug("terminating {}", key)
            value.close().onComplete {
              case Success(_) => log.info("{} terminated", key)
              case Failure(e) => log.error(e, "{} termination fail", key)
            }
          }
        })
        .buildAsync(new AsyncCacheLoader[String, Producer] {
          override def asyncLoad(key: String, executor: Executor): CompletableFuture[Producer] = {
            log.debug("creating {}", key)
            Producer
              .create(key)
              .andThen {
                case Success(_) => log.info("{} created", key)
                case Failure(e) => log.error(e, "fail to create {}", key)
              }
              .toJava
              .toCompletableFuture
          }
        })

    val route =
      (get & path("messages" / Segment)) { topic =>
        parameter("nextToken" ?) { maybeToken =>
          complete {
            cache
              .get(topic)
              .toScala
              .flatMap { producer =>
                producer.next()
              }.map { case (records, next) =>
                val body = Map(
                  "messages" ->
                    records
                      .records
                      .iterator()
                      .asScala
                      .map { record =>
                        Map(
                          "key" -> record.key(),
                          "value" -> record.value()
                        )
                      }.toSeq,
                  "nextToken" -> ""
                )

              println(body)

              HttpEntity(ContentTypes.`application/json`, Serialization.write(body))
            }.andThen {
              case Success(_) => log.info("request for {}/{}", topic, maybeToken.isDefined)
              case Failure(e) =>
                log.error(e, "fail {}/{}", topic, maybeToken.isDefined)
                throw e
            }
          }
        }
      }

    Http().bindAndHandle(route, "localhost", 8080).andThen {
      case Success(_) => log.info("server running at localhost:8080")
      case Failure(e) => log.error(e, "fail to start server")
    }
	}
}