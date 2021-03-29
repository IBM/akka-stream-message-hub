package com.ibm.analytics.messagehub

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model.HttpMethods._
import akka.http.scaladsl.model.MediaTypes._
import akka.http.scaladsl.model._
import akka.http.scaladsl.model.headers._
import akka.http.scaladsl.unmarshalling.Unmarshal
import akka.stream.ActorMaterializer
import com.typesafe.scalalogging.LazyLogging
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future}
import scala.util.Try

/**
  * MesssageHub REST API documentation:
  *
  * https://github.com/ibm-messaging/message-hub-docs/tree/master/admin-rest-api
  *
  */

case class TopicCreationError(statusCode: StatusCode, body: String, message: String, causedBy: Throwable = null) extends Exception(message, causedBy)

case class GetTopicsError(statusCode: StatusCode, body: String, message: String, causedBy: Throwable = null) extends Exception(message, causedBy)

case class DeleteTopicError(statusCode: StatusCode, body: String, message: String, causedBy: Throwable = null) extends Exception(message, causedBy)

case class TopicDetails(name: String, partitions: Int, retentionMs: Long)

object TopicDetails extends DefaultJsonProtocol {
  implicit val format: JsonFormat[TopicDetails] = jsonFormat3(TopicDetails.apply)
}

object Admin {
  def create(vcap: String)(implicit system: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Try[Admin] =
    MessageHub.VCAP.parse(vcap).map { vcap => Admin(vcap) }

  def apply(vcap: MessageHub.VCAP)(implicit system: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Admin =
    Admin(vcap.kafka_admin_url, vcap.api_key)

  def apply(adminUrl: String, serviceKey: String)(implicit system: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Admin =
    new Admin(adminUrl, serviceKey)

  def create(vcap: JsObject)(implicit system: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer): Try[Admin] =
    MessageHub.VCAP.parse(vcap).map { vcap => Admin(vcap) }
}

class Admin(adminUrl: String, serviceKey: String)(implicit system: ActorSystem, ec: ExecutionContext, mat: ActorMaterializer) extends LazyLogging {


  private val authorization = RawHeader("X-Auth-Token", serviceKey)

  def createTopic(name: String, retention: FiniteDuration = 24 hours, partitions: Int = 1, replication: Int = 3): Future[String] = {
    val address = s"$adminUrl/admin/topics"
    val json =
      s"""{
         |  "name": "$name",
         |  "configs": {
         |    "retentionMs": ${retention.toMillis}
         |  },
         |  "partitions": $partitions,
         |  "replicationFactor": $replication
         |}""".stripMargin

    logger.debug(s"Requesting... POST - ${address} /n $json")

    val entity = HttpEntity(`application/json`, json)
    Http().singleRequest(HttpRequest(POST, uri = address, entity = entity, headers = List(authorization)))
      .flatMap { response => Unmarshal(response.entity).to[String].map { body => (response, body) } }
      .flatMap {
        case (response, body) =>
          response.status match {
            case StatusCodes.Accepted =>
              Future.successful(body)
            case _ =>
              logger.error(s"Unable to create topic: $name", body)
              Future.failed(TopicCreationError(response.status, body, s"Unable to create topic: $name"))
          }
      }
  }

  def getTopics: Future[Seq[TopicDetails]] = {
    val address = s"$adminUrl/admin/topics"
    logger.debug(s"Requesting... GET - ${address}")

    Http().singleRequest(HttpRequest(GET, uri = address, headers = List(authorization)))
      .flatMap { response => Unmarshal(response.entity).to[String].map { body => (response, body) } }
      .flatMap {
        case (response, body) =>
          response.status match {
            case StatusCodes.OK =>
              Future.successful(body.parseJson.convertTo[Seq[TopicDetails]])
            case _ =>
              logger.error(s"Unable to get topics", body)
              Future.failed(GetTopicsError(response.status, body, s"Unable to get topics"))
          }
      }
  }

  def deleteTopic(name: String): Future[String] = {
    val address = s"$adminUrl/admin/topics/$name"
    logger.debug(s"Requesting... GET - ${address}")

    Http().singleRequest(HttpRequest(DELETE, uri = address, headers = List(authorization)))
      .flatMap { response => Unmarshal(response.entity).to[String].map { body => (response, body) } }
      .flatMap {
        case (response, body) =>
          response.status match {
            case StatusCodes.Accepted =>
              Future.successful(body)
            case _ =>
              logger.error(s"Unable to delete topic: $name", body)
              Future.failed(DeleteTopicError(response.status, body, s"Unable to delete topic"))
          }
      }
  }
}
