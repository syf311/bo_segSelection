package org.template.segselectionrecommendation

import io.prediction.controller.PPreparator

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD

class Preparator
  extends PPreparator[TrainingData, PreparedData] {

  def prepare(sc: SparkContext, trainingData: TrainingData): PreparedData = {
    new PreparedData(
      users = trainingData.users,
      segs = trainingData.segs,
      connectEvents = trainingData.connectEvents)
  }
}

class PreparedData(
  val users: RDD[(String, User)],
  val segs: RDD[(String, Seg)],
  val connectEvents: RDD[ConnectEvent]
) extends Serializable
