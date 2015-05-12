package org.template.segselectionrecommendation

import io.prediction.controller.IEngineFactory
import io.prediction.controller.Engine

case class Query(
  user: String,
  num: Int,
  whiteList: Option[Set[String]],
  blackList: Option[Set[String]]
) extends Serializable

case class PredictedResult(
  itemScores: Array[SegScore]
) extends Serializable

case class SegScore(
  seg: String,
  score: Double
) extends Serializable

object SegSelectionRecommendationEngine extends IEngineFactory {
  def apply() = {
    new Engine(
      classOf[DataSource],
      classOf[Preparator],
      Map("segSel" -> classOf[SegSelectionAlgorithm]),
      classOf[Serving])
  }
}
