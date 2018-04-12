package com.ibm.analytics.messagehub.actors

import java.util.concurrent.TimeUnit

import akka.{Done, NotUsed}
import akka.actor.{Actor, ActorRef, ActorSystem, PoisonPill, Props, Status}
import akka.kafka.ConsumerMessage.CommittableMessage
import akka.kafka.scaladsl.Consumer
import akka.stream.ActorAttributes.supervisionStrategy
import akka.stream.{ClosedShape, Materializer, Supervision}
import akka.stream.scaladsl.{Broadcast, Flow, GraphDSL, RunnableGraph, Sink, Source}
import com.ibm.analytics.messagehub.models.FlowStats
import com.typesafe.config.ConfigFactory
import org.slf4j.LoggerFactory

import scala.concurrent.Future
import scala.concurrent.duration._
import scala.util.{Failure, Success, Try}

object KafkaStreamRunnerActor {
  private val logger = LoggerFactory.getLogger(getClass)

  type MessageType = CommittableMessage[String, String]
  type ControlType = Consumer.Control

  val DELAY: FiniteDuration = Try {
    FiniteDuration(ConfigFactory.load.getDuration("message-hub.stream-runner.restart.delay", TimeUnit.MILLISECONDS), TimeUnit.MILLISECONDS)
  } getOrElse 60000.millis

  val MAX_PARTITIONS: Int = Try {
    ConfigFactory.load.getInt("message-hub.stream-runner.subscriber.max-partitions")
  } getOrElse 8

  case object CleanupDone
  case object RestartTick

  def runStream(
                 name: String,
                 source: () => Source[MessageType, ControlType],
                 processFlow: Flow[MessageType, MessageType, NotUsed],
                 flowStats: FlowStats
               )
               (implicit system: ActorSystem, mat: Materializer): ActorRef = {
    system.actorOf(props(name, source, processFlow, flowStats), s"StreamRunnerActor-$name-first")
  }

  def props(
             name: String,
             source: () => Source[MessageType, ControlType],
             processFlow: Flow[MessageType, MessageType, NotUsed],
             flowStats: FlowStats
           )
           (implicit mat: Materializer): Props =
    Props(new KafkaStreamRunnerActor(name, source, processFlow, flowStats))
}

class KafkaStreamRunnerActor(
                              name: String,
                              createSource: () => Source[KafkaStreamRunnerActor.MessageType, KafkaStreamRunnerActor.ControlType],
                              processFlow: Flow[KafkaStreamRunnerActor.MessageType, KafkaStreamRunnerActor.MessageType, NotUsed],
                              flowStats: FlowStats
                            )
                            (implicit mat: Materializer) extends Actor {

  import KafkaStreamRunnerActor._

  private val msgPrefix: String = s"[${self.path.name}]:"
  private var controller: ControlType = _

  private val graphSupervisionStrategy: Supervision.Decider = t => {
    logger.warn(s"S$msgPrefix failure inside stream - skip record", t)
    flowStats.registerError(t)
    Supervision.Resume
  }

  private val commitOffsetFlow: Flow[MessageType, Done, NotUsed] =
    Flow[MessageType]
      .groupBy(MAX_PARTITIONS, _.committableOffset.partitionOffset.key.partition)
      .groupedWithin(Int.MaxValue, 100.millis)
      .map { messages =>
        if(messages.nonEmpty) {
          logger.debug(s"$msgPrefix committing bulk of events (${messages.size}): ${messages.last.committableOffset}")
          flowStats.registerCommitted(messages.size,
            messages.last.committableOffset.partitionOffset.key.partition,
            messages.last.committableOffset.partitionOffset.offset
          )
          messages.last.committableOffset.commitScaladsl()
        }
      }.mergeSubstreams.map(_ => Done)

  val commitOffsetSink: Sink[MessageType, NotUsed] =
    Flow[MessageType].via(commitOffsetFlow).to(Sink.ignore)

  def cleanUp(): Future[_] = controller.shutdown()

  /**
    * Graph processing start right after creating actor
    */
  override def preStart(): Unit = {
    super.preStart()
    logger.info(s"$msgPrefix starting stream")
    controller = RunnableGraph.fromGraph(GraphDSL.create(createSource()) { implicit builder => source =>
      import GraphDSL.Implicits._

      val monitor = builder.add(Sink.actorRef(self, Status.Success(())))
      val monitorBroadcast = builder.add(Broadcast[MessageType](2))
      monitorBroadcast <~ source
      monitorBroadcast ~> monitor                           //pass all messages through self
      monitorBroadcast ~> processFlow ~> commitOffsetSink   //process messages and pass to commit sink

      ClosedShape
    }).withAttributes(supervisionStrategy(graphSupervisionStrategy)).run()
  }

  override def receive: Receive = {
    case Status.Success(_) =>
      logger.info(s"$msgPrefix stream completed successfully")
      self ! PoisonPill

    case Status.Failure(e) =>
      logger.warn(s"$msgPrefix failure received - restart in $DELAY", e)
      cleanUp().onComplete {
        case Success(_) =>
          logger.info(s"$msgPrefix clean completed successfully")
          self ! CleanupDone
        case Failure(t) =>
          logger.warn(s"S$msgPrefix clean completed with failure - restart anyway", t)
          self ! CleanupDone
      }(context.system.dispatcher)

    case CleanupDone       =>
      logger.info(s"$msgPrefix cleanup done - restart scheduled in $DELAY")
      context.system.scheduler.scheduleOnce(DELAY)(self ! RestartTick)(context.system.dispatcher)

    case RestartTick       =>
      logger.info(s"$msgPrefix begin restart")
      flowStats.registerRestart()
      context.system.actorOf(props(name, createSource, processFlow, flowStats), s"StreamRunnerActor-$name-$hashCode")
      self ! PoisonPill

    case CommittableMessage(record, offset) =>
      flowStats.registerInflow(offset.partitionOffset.key.partition, offset.partitionOffset.offset, record.timestamp)

    case unknown =>
      logger.warn(s"$msgPrefix unexpected element going through the flow: $unknown")
  }
}

