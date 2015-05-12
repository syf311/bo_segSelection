package org.template.segselectionrecommendation

import io.prediction.controller.PDataSource
import io.prediction.controller.EmptyEvaluationInfo
import io.prediction.controller.EmptyActualResult
import io.prediction.controller.Params
import io.prediction.data.storage.Event
import io.prediction.data.store.PEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

case class DataSourceParams(appName: String) extends Params

class DataSource(val dsp: DataSourceParams)
  extends PDataSource[TrainingData,
      EmptyEvaluationInfo, Query, EmptyActualResult] {

  @transient lazy val logger = Logger[this.type]

  override
  def readTraining(sc: SparkContext): TrainingData = {

    // create a RDD of (entityID, User)
    val usersRDD: RDD[(String, User)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "user"
    )(sc).map { case (entityId, properties) =>
      val user = try {
        User()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" user ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, user)
    }.cache()

    // create a RDD of (entityID, Seg)
    val segsRDD: RDD[(String, Seg)] = PEventStore.aggregateProperties(
      appName = dsp.appName,
      entityType = "seg"
    )(sc).map { case (entityId, properties) =>
      val seg = try {
        Seg()
      } catch {
        case e: Exception => {
          logger.error(s"Failed to get properties ${properties} of" +
            s" seg ${entityId}. Exception: ${e}.")
          throw e
        }
      }
      (entityId, seg)
    }.cache()

    val eventsRDD: RDD[Event] = PEventStore.find(
      appName = dsp.appName,
      entityType = Some("user"),
      eventNames = Some(List("connect")),
      // targetEntityType is optional field of an event.
      targetEntityType = Some(Some("seg")))(sc)
      .cache()

    val connectEventsRDD: RDD[ConnectEvent] = eventsRDD
      .filter { event => event.event == "connect" }
      .map { event =>
        try {
          ConnectEvent(
            user = event.entityId,
            seg = event.targetEntityId.get,
            t = event.eventTime.getMillis
          )
        } catch {
          case e: Exception =>
            logger.error(s"Cannot convert ${event} to ConnectEvent." +
              s" Exception: ${e}.")
            throw e
        }
      }

    new TrainingData(
      users = usersRDD,
      segs = segsRDD,
      connectEvents = connectEventsRDD
    )
  }
}

case class User()

case class Seg()

case class ConnectEvent(user: String, seg: String, t: Long)

class TrainingData(
  val users: RDD[(String, User)],
  val segs: RDD[(String, Seg)],
  val connectEvents: RDD[ConnectEvent]
) extends Serializable {
  override def toString = {
    s"users: [${users.count()} (${users.take(2).toList}...)]" +
    s"segs: [${segs.count()} (${segs.take(2).toList}...)]" +
    s"connectEvents: [${connectEvents.count()}] (${connectEvents.take(2).toList}...)"
  }
}
