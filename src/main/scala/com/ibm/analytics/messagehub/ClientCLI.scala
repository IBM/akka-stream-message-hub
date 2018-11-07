package com.ibm.analytics.messagehub

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.temporal.ChronoField._

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import spray.json._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.Source
import scala.util.{Failure, Success, Try}

/**
  * Created by rbigaj on 06/05/2017.
  */
object ClientCLI {
  implicit val system = ActorSystem("ml-kafka-client-cli")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  def main(args: Array[String]): Unit = {
    if (args.size > 2) {
      val vcap = Source.fromFile(args(0)).mkString
      Subscriber.create(vcap, args(1), "latest") match {
        case Success(subscriber) =>
          sys.addShutdownHook {
            materializer.shutdown
            system.terminate
          }
          subscriber.subscribe(args.drop(2): _*) {
            case consumerRecord =>
              println("-------" * 10)
              val receivedAt = Instant.now
              println(s"Message ${receivedAt}:")
              Try(consumerRecord.value.parseJson) match {
                case Success(msg) =>
                  msg.asJsObject.fields.get("published") match {
                    case Some(JsString(published)) =>
                      val publishedAt = Instant.from(DateTimeFormatter.ISO_DATE_TIME.parse(published))
                      println(s"Published: $publishedAt")
                      val diffMs = (receivedAt.getEpochSecond - publishedAt.getEpochSecond) * 1000 +
                        receivedAt.get(MILLI_OF_SECOND) - publishedAt.get(MILLI_OF_SECOND)
                      println(s"Delay: ${diffMs}ms")
                    case _ =>
                  }
                  println(msg.prettyPrint)
                case Failure(_) =>
                  println("Unable to parse JSON:")
                  println(consumerRecord.value)
              }
              Future.successful(Done)
          }
          println("Waiting for messages...")
        case Failure(exception) =>
          println(s"Invalid MessageHub VCAP file '${args(0)}': ${exception.getMessage}")
      }
    } else {
      println("Usage: java -jar akka-stream-message-hub_XXX.jar <VCAP file> <consumer-group> <topic1> <topic2> ...")
    }
  }
}
