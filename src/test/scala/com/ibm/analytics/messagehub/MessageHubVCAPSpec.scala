package com.ibm.analytics.messagehub

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.{Failure, Success}

/**
  * Created by rbigaj on 10/04/2017.
  */
class MessageHubVCAPSpec extends AnyWordSpec with Matchers {

  val VALID_VCAP =
    s"""
       |{
       |  "instance_id": "bf78ce2b-8b43-4e42-a5d4-a5a8f31fe481",
       |  "mqlight_lookup_url": "https://mqlight-lookup-stage1.messagehub.services.us-south.bluemix.net/Lookup?serviceId=bf78ce2b-8b43-4e42-a5d4-a5a8f31fe481",
       |  "api_key": "9JA7ykkGFaKZFMtJAKMR2xEm7uBazJC8zG55dKXlQAQMKYtw",
       |  "kafka_admin_url": "https://kafka-admin-stage1.messagehub.services.us-south.bluemix.net:443",
       |  "kafka_rest_url": "https://kafka-rest-stage1.messagehub.services.us-south.bluemix.net:443",
       |  "kafka_brokers_sasl": [
       |    "kafka05-stage1.messagehub.services.us-south.bluemix.net:9093",
       |    "kafka04-stage1.messagehub.services.us-south.bluemix.net:9093",
       |    "kafka01-stage1.messagehub.services.us-south.bluemix.net:9093",
       |    "kafka02-stage1.messagehub.services.us-south.bluemix.net:9093",
       |    "kafka03-stage1.messagehub.services.us-south.bluemix.net:9093"
       |  ],
       |  "user": "user1234",
       |  "password": "pass1234"
       |}
       |
     """.stripMargin

  val INVALID_VCAP =
    """
      |{
      |  "kafka_brokers_sasl": "INVALID VCAP",
      |  "user": "user1234",
      |  "password": "4321pass"
      |}
    """.stripMargin

  "MessageHub.VCAP" when {
    "is created with valid data" should {
      "return credentials" in {
        MessageHub.VCAP.parse(VALID_VCAP) match {
          case Success(vcap) =>
            vcap.kafka_brokers_sasl shouldEqual List("kafka05-stage1.messagehub.services.us-south.bluemix.net:9093",
              "kafka04-stage1.messagehub.services.us-south.bluemix.net:9093",
              "kafka01-stage1.messagehub.services.us-south.bluemix.net:9093",
              "kafka02-stage1.messagehub.services.us-south.bluemix.net:9093",
              "kafka03-stage1.messagehub.services.us-south.bluemix.net:9093")
            vcap.user shouldEqual "user1234"
            vcap.password shouldEqual "pass1234"
            vcap.kafka_admin_url shouldEqual "https://kafka-admin-stage1.messagehub.services.us-south.bluemix.net:443"
            vcap.api_key shouldEqual "9JA7ykkGFaKZFMtJAKMR2xEm7uBazJC8zG55dKXlQAQMKYtw"
          case Failure(ex) =>
            throw ex
        }
      }
    }

    "is created with invalid data" should {
      "return failure" in {
        MessageHub.VCAP.parse(INVALID_VCAP) match {
          case Success(vcap) =>
            fail("Invalid VCAP should not be accepted")
          case Failure(ex) =>
            ex shouldBe a[MessageHub.InvalidVCAPError]
        }
      }
    }
  }
}
