package com.ibm.analytics.messagehub

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import spray.json._

import scala.concurrent.{ExecutionContextExecutor, Future}
import scala.io.Source
import scala.util.{Failure, Success}

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
      Subscriber.create(vcap, args(1)) match {
        case Success(subscriber) =>
          sys.addShutdownHook {
            materializer.shutdown
            system.terminate
          }
          subscriber.subscribe(args.drop(2): _*) {
            case consumerRecord =>
              println("-------" * 10)
              println("Message:")
              println(consumerRecord.value.parseJson.prettyPrint)
              Future.successful(Done)
          }
          println("Waiting for messages...")
        case Failure(exception) =>
          println(s"Invalid MessageHub VCAP file '${args(0)}': ${exception.getMessage}")
      }
    } else {
      println("Usage: java -jar ml-kafka-client-XXX.jar <VCAP file> <consumer-group> <topic1> <topic2> ...")
    }
  }
}
