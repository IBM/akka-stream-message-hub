package com.ibm.analytics.messagehub

import com.typesafe.config._

object KafkaConfigFactory {
  def load(): Config = ConfigFactory.load("akka.kafka.conf")
}
