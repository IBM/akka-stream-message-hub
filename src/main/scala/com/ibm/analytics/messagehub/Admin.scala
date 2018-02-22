package com.ibm.analytics.messagehub

import com.typesafe.scalalogging.slf4j.Logger
import org.apache.http.HttpException
import org.apache.http.client.methods.{HttpDelete, HttpGet, HttpPost}
import org.apache.http.client.utils.URIBuilder
import org.apache.http.conn.ssl.SSLConnectionSocketFactory
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.{CloseableHttpClient, HttpClients}
import org.apache.http.util.EntityUtils
import org.slf4j.LoggerFactory
import spray.json.DefaultJsonProtocol._
import spray.json._

import scala.util.{Failure, Success, Try}

/**
  * Created by msarto on 10.05.2017.
  *
  * Based on https://github.com/ibm-messaging/message-hub-docs/tree/master/admin-rest-api
  *
  */
object Admin {
  def create(vcap: String): Try[Admin] = MessageHub.VCAP.parse(vcap).map { vcap => Admin(vcap) }

  def apply(vcap: MessageHub.VCAP): Admin = Admin(vcap.kafka_admin_url, vcap.api_key)

  def apply(adminUrl: String, serviceKey: String): Admin = new Admin(adminUrl, serviceKey)

  def create(vcap: JsObject): Try[Admin] = MessageHub.VCAP.parse(vcap).map { vcap => Admin(vcap) }
}

class Admin(adminUrl: String, serviceKey: String) {
  private lazy val logger = Logger(LoggerFactory.getLogger(this.getClass))

  def createTopic(name: String, retentionMs: Long = 86400000, partitions: Int = 1, replication: Int = 3): Try[String] = {
    val address = s"$adminUrl/admin/topics"
    val json =
      s"""{
         |  "name": "$name",
         |  "configs": {
         |    "retentionMs": $retentionMs
         |  },
         |  "partitions": $partitions,
         |  "replicationFactor": $replication
         |}""".stripMargin
    val httpClient = getHttpsClient
    val httpPost = new HttpPost(address)
    val uriBuilder = new URIBuilder(httpPost.getURI)
    httpPost.setURI(uriBuilder.build)
    httpPost.addHeader("X-Auth-Token", serviceKey)
    val strEntity: StringEntity = new StringEntity(json)
    strEntity.setContentType("application/json")
    httpPost.setEntity(strEntity)
    logger.debug(s"Requesting... POST - ${address} /n $json")
    val t0 = System.currentTimeMillis()
    try {
      val response = httpClient.execute(httpPost)
      val statusCode = response.getStatusLine.getStatusCode
      val resp = EntityUtils.toString(response.getEntity)
      response.close
      val duration = System.currentTimeMillis() - t0
      logger.debug(s"POST [$duration ms] - ${address} & - ${statusCode} - ${response.getAllHeaders} :\n $resp")
      if (statusCode == 202) Success(resp) else Failure(new HttpException(s"HTTP $statusCode " + resp))
    } finally {
      httpClient.close
    }
  }

  def getTopics(): Try[Seq[String]] = {
    val address = s"$adminUrl/admin/topics"
    val httpClient = getHttpsClient
    val httpGet = new HttpGet(address)
    val uriBuilder = new URIBuilder(httpGet.getURI)
    httpGet.setURI(uriBuilder.build)
    httpGet.addHeader("X-Auth-Token", serviceKey)
    logger.debug(s"Requesting... GET - ${address}")
    val t0 = System.currentTimeMillis()
    try {
      val response = httpClient.execute(httpGet)
      val statusCode = response.getStatusLine.getStatusCode
      val resp = EntityUtils.toString(response.getEntity)
      response.close
      val duration = System.currentTimeMillis() - t0
      logger.debug(s"GET [$duration ms] - ${address} & - ${statusCode} - ${response.getAllHeaders} :\n $resp")
      if (statusCode == 200) {
        val items = resp.parseJson.asInstanceOf[JsArray].elements map { item => item.asJsObject.fields.get("name").get.convertTo[String] }
        Success(items)
      } else Failure(new HttpException(s"HTTP $statusCode " + resp))
    } finally {
      httpClient.close
    }
  }

  def deleteTopic(name: String): Try[String] = {
    val address = s"$adminUrl/admin/topics/$name"
    val httpClient = getHttpsClient
    val httpDelete = new HttpDelete(address)
    val uriBuilder = new URIBuilder(httpDelete.getURI)
    httpDelete.setURI(uriBuilder.build)
    httpDelete.addHeader("X-Auth-Token", serviceKey)
    logger.debug(s"Requesting... DELETE - ${address}")
    val t0 = System.currentTimeMillis()
    try {
      val response = httpClient.execute(httpDelete)
      val statusCode = response.getStatusLine.getStatusCode
      val resp = EntityUtils.toString(response.getEntity)
      response.close
      val duration = System.currentTimeMillis() - t0
      logger.debug(s"DELETE [$duration ms] - ${address} & - ${statusCode} - ${response.getAllHeaders} :\n $resp")
      if (statusCode == 202 || statusCode == 422) Success(resp) else Failure(new HttpException(s"HTTP $statusCode " + resp))
    } finally {
      httpClient.close
    }
  }

  protected def getHttpsClient(): CloseableHttpClient = {
    HttpClients.custom()
      .setHostnameVerifier(SSLConnectionSocketFactory.ALLOW_ALL_HOSTNAME_VERIFIER)
      .build()
  }

}
