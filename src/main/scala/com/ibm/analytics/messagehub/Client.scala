package com.ibm.analytics.messagehub

import java.util.concurrent.TimeUnit

import akka.actor.ActorSystem
import akka.kafka.ConsumerMessage._
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.kafka.{ConsumerMessage, ConsumerSettings, ProducerSettings, Subscriptions}
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import akka.stream.{ActorMaterializer, KillSwitches, SharedKillSwitch}
import akka.{Done, NotUsed}
import com.ibm.analytics.messagehub.Subscriber.SubscriberSettings
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}
import org.apache.kafka.common.config.{SaslConfigs, SslConfigs}
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.slf4j.LoggerFactory
import spray.json._

import scala.concurrent.duration._
import scala.concurrent.{ExecutionContext, Future, Promise}
import scala.util.{Failure, Success, Try}

object MessageHub {

  private[messagehub] def createJaasConfigContent(user: String, password: String): String = {
    val template = """org.apache.kafka.common.security.plain.PlainLoginModule required serviceName="kafka" username="{{USER}}" password="{{PASSWORD}}";"""
    val content = template.replace("{{USER}}", user).replace("{{PASSWORD}}", password)
    content
  }

  case class InvalidVCAPError(message: String, causedBy: Throwable = null) extends Exception(message, causedBy)

  case class VCAP(kafka_brokers_sasl: List[String], user: String, password: String, kafka_admin_url: String, api_key: String)

  object VCAP extends DefaultJsonProtocol {
    implicit val format: RootJsonFormat[VCAP] = jsonFormat5(VCAP.apply)

    val recoverDeserializationException: PartialFunction[Throwable, Try[VCAP]] = {
      case e: DeserializationException => Failure(InvalidVCAPError(s"Invalid MessageHub VCAP fields: ${e.getMessage}", e))
    }

    def parse(vcap: String): Try[VCAP] = Try(vcap.parseJson.convertTo[VCAP]).recoverWith(recoverDeserializationException)

    def parse(vcap: JsObject): Try[VCAP] = Try(vcap.convertTo[VCAP]).recoverWith(recoverDeserializationException)
  }
}

case class TruststoreConfig(location: String, password: String) {
  def toProperties: Map[String,String] = if (location.isEmpty) {
    Map.empty
  } else {
    Map(SslConfigs.SSL_TRUSTSTORE_LOCATION_CONFIG -> location,
      SslConfigs.SSL_TRUSTSTORE_PASSWORD_CONFIG -> password)
  }
}

object TruststoreConfig {
  private val config = ConfigFactory.load()

  def load(): Option[TruststoreConfig] =
    Try(config.getConfig("akka.event-streams.truststore")).toOption.map { tsConfig =>
      TruststoreConfig(tsConfig.getString("location"), tsConfig.getString("password"))
    }

  def properties(maybeTruststoreConfig: Option[TruststoreConfig]): Map[String, String] = {
    val truststoreConfig = maybeTruststoreConfig.orElse(load())
    truststoreConfig.map(_.toProperties).getOrElse(Map.empty)
  }
}

object Publisher {
  def create(vcap: String): Try[Publisher] = MessageHub.VCAP.parse(vcap).map { vcap => Publisher(vcap) }

  def create(vcap: String, truststoreConfig: TruststoreConfig): Try[Publisher] =
    MessageHub.VCAP.parse(vcap).map { vcap => Publisher(vcap, truststoreConfig) }

  def apply(vcap: MessageHub.VCAP): Publisher = Publisher(vcap.kafka_brokers_sasl, vcap.user, vcap.password)

  def apply(vcap: MessageHub.VCAP, truststoreConfig: TruststoreConfig): Publisher =
    Publisher(vcap.kafka_brokers_sasl, vcap.user, vcap.password, truststoreConfig)

  def apply(kafkaBrokers: Seq[String], user: String, password: String): Publisher =
    new Publisher(kafkaBrokers.mkString(","), user, password)

  def apply(kafkaBrokers: Seq[String], user: String, password: String, truststoreConfig: TruststoreConfig): Publisher =
    new Publisher(kafkaBrokers.mkString(","), user, password, Some(truststoreConfig))

  def create(vcap: JsObject): Try[Publisher] = MessageHub.VCAP.parse(vcap).map { vcap => Publisher(vcap) }

  def create(vcap: JsObject, truststoreConfig: TruststoreConfig): Try[Publisher] =
    MessageHub.VCAP.parse(vcap).map { vcap => Publisher(vcap, truststoreConfig) }

  def apply(kafkaBrokers: String, user: String, password: String): Publisher =
    new Publisher(kafkaBrokers, user, password)

  def apply(kafkaBrokers: String, user: String, password: String, truststoreConfig: TruststoreConfig): Publisher =
    new Publisher(kafkaBrokers, user, password, Some(truststoreConfig))
}

class Publisher(kafkaBrokers: String, user: String, password: String, maybeTruststoreConfig: Option[TruststoreConfig] = None) {
  private val producerConfig = KafkaConfigFactory.load().getConfig("akka.kafka.producer")

  private val producerProperties = TruststoreConfig.properties(maybeTruststoreConfig) ++ Map(
    SaslConfigs.SASL_JAAS_CONFIG -> MessageHub.createJaasConfigContent(user, password)
  )

  val producerSettings: ProducerSettings[String, String] =
    ProducerSettings(producerConfig, new StringSerializer, new StringSerializer)
      .withBootstrapServers(kafkaBrokers)
      .withProperties(producerProperties)

  val kafkaProducer: KafkaProducer[String, String] = producerSettings.createKafkaProducer()

  val producerSink: Sink[ProducerRecord[String, String], Future[Done]] = Producer.plainSink(producerSettings, kafkaProducer)

  def publish(topic: String, msg: JsObject)
             (implicit ec: ExecutionContext,
              materializer: ActorMaterializer): Future[Done] =
    publish(Source.single(new ProducerRecord[String, String](topic, msg.toString)))

  def publish(source: Source[ProducerRecord[String, String], NotUsed])
             (implicit ec: ExecutionContext,
              materializer: ActorMaterializer): Future[Done] = {
    source.runWith(producerSink)
  }

  def close(): Unit = {
    kafkaProducer.flush()
    kafkaProducer.close()
  }
}

case class SubscriberResult(private[messagehub] val _done: Promise[Done], private[messagehub] var _killSwitch: SharedKillSwitch, private[messagehub] var _sourceControl: Option[Consumer.Control] = None) {
  def killSwitch: SharedKillSwitch = _killSwitch

  def sourceControl: Option[Consumer.Control] = _sourceControl

  def future: Future[Done] = _done.future
}

object Subscriber {
  private val config = ConfigFactory.load()

  private val Millis = TimeUnit.MILLISECONDS

  private val Parallel: Int = Try {
    config.getInt("akka.event-streams.parallel")
  } getOrElse 10

  private val CommitMaxRecords: Int = Try {
    config.getInt("akka.event-streams.commit.max-records")
  } getOrElse 50

  private val CommitDelay: FiniteDuration = Try {
    FiniteDuration(config.getDuration("akka.event-streams.commit.max-delay", Millis), Millis)
  } getOrElse 3.seconds

  private val AutoRestart: Boolean = Try {
    config.getBoolean("akka.event-streams.restart.auto")
  } getOrElse true

  private val RestartDelay: FiniteDuration = Try {
    FiniteDuration(config.getDuration("akka.event-streams.restart.delay", Millis), Millis)
  } getOrElse 60.seconds

  Try(config.getConfig("akka.event-streams.truststore")).toOption.map { tsConfig =>

  }

  private val TruststoreLocation: Option[String] = Try {
    config.getString("akka.event-streams.truststore.location")
  }.toOption

  def create(vcap: String, consumerGroup: String): Try[Subscriber] =
    MessageHub.VCAP.parse(vcap).map { vcap => Subscriber(vcap, consumerGroup) }

  def create(vcap: String, consumerGroup: String, offset: String):
  Try[Subscriber] = MessageHub.VCAP.parse(vcap).map { vcap => Subscriber(vcap, consumerGroup, offset) }

  def create(vcap: String, consumerGroup: String, offset: String, truststoreConfig: TruststoreConfig):
  Try[Subscriber] = MessageHub.VCAP.parse(vcap).map { vcap => Subscriber(vcap, consumerGroup, offset, truststoreConfig) }

  def create(vcap: JsObject, consumerGroup: String): Try[Subscriber] =
    MessageHub.VCAP.parse(vcap).map { vcap => Subscriber(vcap, consumerGroup) }

  def create(vcap: JsObject, consumerGroup: String, offset: String): Try[Subscriber] =
    MessageHub.VCAP.parse(vcap).map { vcap => Subscriber(vcap, consumerGroup, offset) }

  def create(vcap: JsObject, consumerGroup: String, offset: String, truststoreConfig: TruststoreConfig): Try[Subscriber] =
    MessageHub.VCAP.parse(vcap).map { vcap => Subscriber(vcap, consumerGroup, offset, truststoreConfig) }

  def apply(vcap: MessageHub.VCAP, consumerGroup: String, offset: String, truststoreConfig: TruststoreConfig): Subscriber =
    Subscriber(vcap.kafka_brokers_sasl, vcap.user, vcap.password, consumerGroup, offset, truststoreConfig)

  def apply(vcap: MessageHub.VCAP, consumerGroup: String, offset: String): Subscriber =
    Subscriber(vcap.kafka_brokers_sasl, vcap.user, vcap.password, consumerGroup, offset)

  def apply(vcap: MessageHub.VCAP, consumerGroup: String): Subscriber =
    Subscriber(vcap.kafka_brokers_sasl, vcap.user, vcap.password, consumerGroup)

  def apply(kafkaBrokers: Seq[String], user: String, password: String, consumerGroup: String, offset: String, truststoreConfig: TruststoreConfig): Subscriber =
    new Subscriber(kafkaBrokers.mkString(","), user, password, consumerGroup, offset, Some(truststoreConfig))

  def apply(kafkaBrokers: Seq[String], user: String, password: String, consumerGroup: String, offset: String): Subscriber =
    new Subscriber(kafkaBrokers.mkString(","), user, password, consumerGroup, offset)

  def apply(kafkaBrokers: Seq[String], user: String, password: String, consumerGroup: String): Subscriber =
    new Subscriber(kafkaBrokers.mkString(","), user, password, consumerGroup)

  def apply(kafkaBrokers: String, user: String, password: String, consumerGroup: String): Subscriber =
    new Subscriber(kafkaBrokers, user, password, consumerGroup)

  def withNoOffsetCommit(kafkaBrokers: String, user: String, password: String, consumerGroup: String, offset: String, truststoreConfig: TruststoreConfig): Subscriber =
    new Subscriber(kafkaBrokers, user, password, consumerGroup, offset, Some(truststoreConfig))

  def withNoOffsetCommit(kafkaBrokers: String, user: String, password: String, consumerGroup: String, offset: String): Subscriber =
    new Subscriber(kafkaBrokers, user, password, consumerGroup, offset)

  def settings(autoRestart: Boolean = AutoRestart, restartDelay: FiniteDuration = RestartDelay) =
    SubscriberSettings(autoRestart, restartDelay)

  case class SubscriberSettings(autoRestart: Boolean = AutoRestart,
                                restartDelay: FiniteDuration = RestartDelay,
                                result: SubscriberResult = SubscriberResult(Promise[Done], KillSwitches.shared("ml-kafka-subscriber-kill-switch")))

}

class Subscriber(kafkaBrokers: String, user: String, password: String, consumerGroup: String, offset: String = "earliest",
                 maybeTruststoreConfig: Option[TruststoreConfig] = None) {

  private val consumerConfig = KafkaConfigFactory.load().getConfig("akka.kafka.consumer")
  private val logger = LoggerFactory.getLogger(this.getClass)

  private val consumerProperties = TruststoreConfig.properties(maybeTruststoreConfig) ++ Map(
    SaslConfigs.SASL_JAAS_CONFIG -> MessageHub.createJaasConfigContent(user, password),
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> offset
  )

  val consumerSettings: ConsumerSettings[String, String] = ConsumerSettings(consumerConfig, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(kafkaBrokers)
    .withProperties(consumerProperties)
    .withGroupId(consumerGroup)

  def subscribe(settings: SubscriberSettings, topics: String*)
               (fn: (ConsumerRecord[String, String]) => Future[Done])
               (implicit system: ActorSystem,
                ec: ExecutionContext,
                materializer: ActorMaterializer): Future[Done] = {

    val source = subscribeWithCommittableSource(topics: _*)
    val flow = createSubscriberFlow(fn)
    val done = source.via(flow).runWith(Sink.ignore)

    val restart: () => Unit = () => {
      subscribe(settings, topics: _*)(fn)
    }
    handleRestart(done, settings, restart)

    settings.result._done.future
  }

  def subscribe(topics: String*)(fn: (ConsumerRecord[String, String]) => Future[Done])
               (implicit system: ActorSystem,
                ec: ExecutionContext,
                materializer: ActorMaterializer): Future[Done] = {
    subscribe(SubscriberSettings(), topics: _*)(fn)
  }

  def subscribeWithNoOffsetCommit(settings: SubscriberSettings, topics: String*)
                                 (fn: (ConsumerRecord[String, String]) => Future[Done])
                                 (implicit system: ActorSystem,
                                  ec: ExecutionContext,
                                  materializer: ActorMaterializer): Future[Done] = {

    val source = subscribeWithPlainSource(topics: _*)
    val flow = createPlainSubscriberFlow(fn)
    val done = source.via(flow).runWith(Sink.ignore)

    val restart: () => Unit = () => {
      subscribe(settings, topics: _*)(fn)
    }
    handleRestart(done, settings, restart)

    settings.result._done.future
  }

  def subscribeWithNoOffsetCommit(topics: String*)(fn: (ConsumerRecord[String, String]) => Future[Done])
                                 (implicit system: ActorSystem,
                                  ec: ExecutionContext,
                                  materializer: ActorMaterializer): Future[Done] = {
    subscribeWithNoOffsetCommit(SubscriberSettings(), topics: _*)(fn)
  }

  def subscribeWithKillSwitch(settings: SubscriberSettings, topics: String*)
                             (fn: (ConsumerRecord[String, String]) => Future[Done])
                             (implicit system: ActorSystem,
                              ec: ExecutionContext,
                              materializer: ActorMaterializer): SubscriberResult = {
    val source = subscribeWithCommittableSource(topics: _*)
    val flow = createSubscriberFlow(fn)
    // ensure that kill switch is re-created after restart
    settings.result._killSwitch = KillSwitches.shared("ml-kafka-subscriber-kill-switch")

    val (control, done) =
      source.via(flow)
        .viaMat(settings.result.killSwitch.flow[Done])(Keep.left)
        .toMat(Sink.ignore)(Keep.both).run()

    settings.result._sourceControl = Some(control)

    val restart: () => Unit = () => {
      subscribeWithKillSwitch(settings, topics: _*)(fn)
    }
    handleRestart(done, settings, restart)

    settings.result
  }

  def subscribeWithCommittableSource(topics: String*): Source[CommittableMessage[String, String], Consumer.Control] =
    Consumer.committableSource(consumerSettings, Subscriptions.topics(topics.toSet))

  def subscribeWithPlainSource(topics: String*): Source[ConsumerRecord[String, String], Consumer.Control] =
    Consumer.plainSource(consumerSettings, Subscriptions.topics(topics.toSet))

  private def createSubscriberFlow(fn: (ConsumerRecord[String, String]) => Future[Done])
                                  (implicit ec: ExecutionContext,
                                   materializer: ActorMaterializer) = {
    Flow[ConsumerMessage.CommittableMessage[String, String]]
      .mapAsync(Subscriber.Parallel) { msg =>
        fn(msg.record).map { _ => msg }
      }
      .groupedWithin(Subscriber.CommitMaxRecords, Subscriber.CommitDelay) // commit every n seconds or m msgs
      .map(group => group.foldLeft(CommittableOffsetBatch.empty) {
      (batch, elem) => batch.updated(elem.committableOffset)
    })
      .mapAsync(3)(_.commitScaladsl())
  }

  private def createPlainSubscriberFlow(fn: (ConsumerRecord[String, String]) => Future[Done])
                                       (implicit ec: ExecutionContext,
                                        materializer: ActorMaterializer) = {
    Flow[ConsumerRecord[String, String]]
      .mapAsync(Subscriber.Parallel) { record =>
        fn(record).map { _ => record }
      }
  }

  private def handleRestart(done: Future[Done], settings: SubscriberSettings, restart: () => Unit)
                           (implicit system: ActorSystem,
                            ec: ExecutionContext,
                            materializer: ActorMaterializer): Unit = {

    val scheduleRestart = () => {
      logger.warn(s"Restarting kafka subscriber in ${settings.restartDelay.toSeconds}s...")
      system.scheduler.scheduleOnce(settings.restartDelay) {
        restart()
      }
    }

    done onComplete {
      case Success(_) =>
        logger.warn(s"Kafka subscriber has been terminated.")
        settings.result._done.trySuccess(Done)
      case Failure(ex) =>
        logger.warn(s"Subscriber failed with:", ex)
        (settings.result.sourceControl match {
          case Some(control) =>
            control.shutdown()
          case None =>
            Future.successful(Done)
        }).recoverWith {
          case ex =>
            logger.warn("Subscriber shutdown failed with ex: ", ex)
            Future.successful(Done)
        }.map { res =>
          Try {
            settings.result.killSwitch.shutdown()
          }.recoverWith {
            case ex =>
              logger.warn("Graph shutdown failed with ex: ", ex)
              Success({})
          }
          if (settings.autoRestart) {
            scheduleRestart()
          } else {
            settings.result._done.tryFailure(ex)
          }
        }
    }
  }
}
