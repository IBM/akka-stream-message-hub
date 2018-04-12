package com.ibm.analytics.messagehub.models

import java.util.concurrent.atomic.AtomicLong


case class MessageRecord(partition: Int, offset: Long, timestamp: Long)

case class ErrorDetails(`class`: String, message: String)

case class CommittedOffset(partition: Int, offset: Long)

case class FlowStats (
                       begin: Long,
                       inflow: AtomicLong,
                       committed_count: AtomicLong,
                       error_count: AtomicLong,
                       restart_count: AtomicLong,
                       var last_received: Option[MessageRecord],
                       var last_error: Option[ErrorDetails],
                       var last_restart: Option[Long],
                       var last_committed: Option[Seq[CommittedOffset]]
                     ) {
  def registerInflow(partition: Int, offset: Long, timestamp: Long): Unit = {
    inflow.incrementAndGet()
    last_received = Some(MessageRecord(partition, offset, timestamp))
  }

  def registerCommitted(count: Long, partition: Int, offset: Long): Unit = {
    committed_count.addAndGet(count)
    val rec = CommittedOffset(partition, offset)
    last_committed = Some(
      last_committed.map(seq => seq.filter(_.partition != partition) :+ rec).getOrElse(Seq(rec))
    )
  }

  def registerError(exc: Throwable): Unit = {
    error_count.incrementAndGet()
    last_error = Some(ErrorDetails(exc.getClass.getCanonicalName, exc.getMessage))
  }

  def registerRestart(): Unit = {
    restart_count.incrementAndGet()
    last_restart = Some(System.currentTimeMillis())
  }
}

object FlowStats {
  def empty: FlowStats = FlowStats(
    System.currentTimeMillis(),
    new AtomicLong(0),
    new AtomicLong(0),
    new AtomicLong(0),
    new AtomicLong(0),
    None,
    None,
    None,
    None
  )
}

