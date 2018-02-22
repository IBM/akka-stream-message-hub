package com.ibm.analytics.messagehub

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl._
import org.apache.kafka.clients.producer.ProducerRecord
import org.scalatest.{Matchers, WordSpec}
import spray.json.{JsNumber, JsObject}

import scala.concurrent._
import scala.concurrent.duration._


class PubSubSpec extends WordSpec with Matchers {
  implicit val system = ActorSystem("ml-kafka-client-test")
  implicit val materializer = ActorMaterializer()
  implicit val executionContext: ExecutionContextExecutor = system.dispatcher

  /* MessageHub-MokshaTest (AA_KRK/dev) */
  val VCAP = scala.io.Source.fromURL(getClass.getResource("/messagehub.vcap")).mkString

  val topic = "ml-kafka-client-test-2"
  val TIMEOUT = 600 seconds
  val COUNT = 10
  var admin: Admin = null
  var publisher: Publisher = null
  var subscriber: Subscriber = null

  "Admin" should {
    "be created from VCAP" in {
      admin = Admin.create(VCAP).get
    }

    s"create topic $topic" in {
      admin.createTopic(topic, 3600000, 4)
    }

    "should list topic" in {
      admin.getTopics.get should contain(topic)
    }
  }

  "Publisher" should {
    "be created from VCAP" in {
      publisher = Publisher.create(VCAP).get
    }

    s"publish $COUNT messages" in {
      val source = Source(1 to COUNT)
        .map(_.toString)
        .map { elem =>
          new ProducerRecord[String, String](topic,
            JsObject(
              "metric" -> JsNumber(scala.util.Random.nextInt(1000)),
              "iteration" -> JsNumber(elem)
            ).toString
          )
        }
      val future = publisher.publish(source)
      Await.result(future, TIMEOUT)
    }
  }

  "Subscriber" should {
    "be created from VCAP" in {
      subscriber = Subscriber.create(VCAP, "test-group-1").get
    }

    s"consume $COUNT messages" in {
      var count = 0
      val p = Promise[Int]()

      subscriber.subscribe(topic) { _ =>
        count += 1
        if (count == COUNT) {
          p success count
        }
        Future.successful(Done)
      }

      Await.result(p.future, TIMEOUT)
    }
  }

  "Admin" should {
    s"delete topic $topic" in {
      admin.deleteTopic(topic)
      Thread.sleep(500)
    }

    "should not list topic" in {
      admin.getTopics.get should not contain (topic)
    }
  }
}
