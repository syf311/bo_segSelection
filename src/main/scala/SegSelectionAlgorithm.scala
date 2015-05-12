package org.template.segselectionrecommendation

import io.prediction.controller.P2LAlgorithm
import io.prediction.controller.Params
import io.prediction.data.storage.BiMap
import io.prediction.data.storage.Event
import io.prediction.data.store.LEventStore

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.{Rating => MLlibRating}
import org.apache.spark.rdd.RDD

import grizzled.slf4j.Logger

import scala.collection.mutable.PriorityQueue
import scala.concurrent.duration.Duration
import scala.concurrent.ExecutionContext.Implicits.global

case class SegSelectionAlgorithmParams(
  appName: String,
  unseenOnly: Boolean,
  seenEvents: List[String],
  similarEvents: List[String],
  rank: Int,
  numIterations: Int,
  lambda: Double,
  seed: Option[Long]
) extends Params


case class SegModel(
  seg: Seg,
  features: Option[Array[Double]], // features by ALS
  count: Int // popular count for default score
)

class SegSelectionModel(
  val rank: Int,
  val userFeatures: Map[Int, Array[Double]],
  val segModels: Map[Int, SegModel],
  val userStringIntMap: BiMap[String, Int],
  val segStringIntMap: BiMap[String, Int]
) extends Serializable {

  @transient lazy val segIntStringMap = segStringIntMap.inverse

  override def toString = {
    s" rank: ${rank}" +
    s" userFeatures: [${userFeatures.size}]" +
    s"(${userFeatures.take(2).toList}...)" +
    s" segModels: [${segModels.size}]" +
    s"(${segModels.take(2).toList}...)" +
    s" userStringIntMap: [${userStringIntMap.size}]" +
    s"(${userStringIntMap.take(2).toString}...)]" +
    s" segStringIntMap: [${segStringIntMap.size}]" +
    s"(${segStringIntMap.take(2).toString}...)]"
  }
}

class SegSelectionAlgorithm(val ap: SegSelectionAlgorithmParams)
  extends P2LAlgorithm[PreparedData, SegSelectionModel, Query, PredictedResult] {

  @transient lazy val logger = Logger[this.type]

  def train(sc: SparkContext, data: PreparedData): SegSelectionModel = {
    require(!data.connectEvents.take(1).isEmpty,
      s"connectEvents in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.users.take(1).isEmpty,
      s"users in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    require(!data.segs.take(1).isEmpty,
      s"segs in PreparedData cannot be empty." +
      " Please check if DataSource generates TrainingData" +
      " and Preprator generates PreparedData correctly.")
    // create User and seg's String ID to integer index BiMap
    val userStringIntMap = BiMap.stringInt(data.users.keys)
    val segStringIntMap = BiMap.stringInt(data.segs.keys)

    val mllibRatings: RDD[MLlibRating] = genMLlibRating(
      userStringIntMap = userStringIntMap,
      segStringIntMap = segStringIntMap,
      data = data
    )

    // MLLib ALS cannot handle empty training data.
    require(!mllibRatings.take(1).isEmpty,
      s"mllibRatings cannot be empty." +
      " Please check if your events contain valid user and seg ID.")

    // seed for MLlib ALS
    val seed = ap.seed.getOrElse(System.nanoTime)

    // use ALS to train feature vectors
    val m = ALS.trainImplicit(
      ratings = mllibRatings,
      rank = ap.rank,
      iterations = ap.numIterations,
      lambda = ap.lambda,
      blocks = -1,
      alpha = 1.0,
      seed = seed)

    val userFeatures = m.userFeatures.collectAsMap.toMap

    // convert ID to Int index
    val segs = data.segs.map { case (id, seg) =>
      (segStringIntMap(id), seg)
    }

    // join seg with the trained segFeatures
    val segFeatures = segs.leftOuterJoin(m.productFeatures).collectAsMap.toMap

    val popularCount = trainDefault(
      userStringIntMap = userStringIntMap,
      segStringIntMap = segStringIntMap,
      data = data
    )

    val segModels: Map[Int, SegModel] = segFeatures
      .map { case (index, (seg, features)) =>
        val sm = SegModel(
          seg = seg,
          features = features,
          // NOTE: use getOrElse because popularCount may not contain all segs.
          count = popularCount.getOrElse(index, 0)
        )
        (index, sm)
      }

    new SegSelectionModel(
      rank = m.rank,
      userFeatures = userFeatures,
      segModels = segModels,
      userStringIntMap = userStringIntMap,
      segStringIntMap = segStringIntMap
    )
  }

  /** Generate MLlibRating from PreparedData.
    * You may customize this function if use different events or different aggregation method
    */
  def genMLlibRating(
    userStringIntMap: BiMap[String, Int],
    segStringIntMap: BiMap[String, Int],
    data: PreparedData): RDD[MLlibRating] = {

    val mllibRatings = data.connectEvents
      .map { r =>
        // Convert user and seg String IDs to Int index for MLlib
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val sindex = segStringIntMap.getOrElse(r.seg, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (sindex == -1)
          logger.info(s"Couldn't convert nonexistent seg ID ${r.seg}"
            + " to Int index.")

        ((uindex, sindex), 1)
      }
      .filter { case ((u, s), v) =>
        // keep events with valid user and seg index
        (u != -1) && (s != -1)
      }
      .reduceByKey(_ + _) // aggregate all connect events of same user-seg pair
      .map { case ((u, s), v) =>
        // MLlibRating requires integer index for user and seg
        MLlibRating(u, s, v)
      }
      .cache()

    mllibRatings
  }

  /** Train default model.
    * You may customize this function if use different events or
    * need different ways to count "popular" score or return default score for seg.
    */
  def trainDefault(
    userStringIntMap: BiMap[String, Int],
    segStringIntMap: BiMap[String, Int],
    data: PreparedData): Map[Int, Int] = {
    // count number of buys
    // (seg index, count)
    val connectCountsRDD: RDD[(Int, Int)] = data.connectEvents
      .map { r =>
        // Convert user and seg String IDs to Int index
        val uindex = userStringIntMap.getOrElse(r.user, -1)
        val sindex = segStringIntMap.getOrElse(r.seg, -1)

        if (uindex == -1)
          logger.info(s"Couldn't convert nonexistent user ID ${r.user}"
            + " to Int index.")

        if (sindex == -1)
          logger.info(s"Couldn't convert nonexistent seg ID ${r.seg}"
            + " to Int index.")

        (uindex, sindex, 1)
      }
      .filter { case (u, s, v) =>
        // keep events with valid user and seg index
        (u != -1) && (s != -1)
      }
      .map { case (u, s, v) => (s, 1) } // key is seg
      .reduceByKey{ case (a, b) => a + b } // count number of segs occurrence

    connectCountsRDD.collectAsMap.toMap
  }

  def predict(model: SegSelectionModel, query: Query): PredictedResult = {

    val userFeatures = model.userFeatures
    val segModels = model.segModels

    // convert whiteList's string ID to integer index
    val whiteList: Option[Set[Int]] = query.whiteList.map( set =>
      set.flatMap(model.segStringIntMap.get(_))
    )

    val finalBlackList: Set[Int] = genBlackList(query = query)
      // convert seen Segs list from String ID to interger Index
      .flatMap(x => model.segStringIntMap.get(x))

    val userFeature: Option[Array[Double]] =
      model.userStringIntMap.get(query.user).flatMap { userIndex =>
        userFeatures.get(userIndex)
      }

    val topScores: Array[(Int, Double)] = if (userFeature.isDefined) {
      // the user has feature vector
      predictKnownUser(
        userFeature = userFeature.get,
        segModels = segModels,
        query = query,
        whiteList = whiteList,
        blackList = finalBlackList
      )
    } else {
      // the user doesn't have feature vector.
      // For example, new user is created after model is trained.
      logger.info(s"No userFeature found for user ${query.user}.")

      // check if the user has recent events on some segs
      val recentSegs: Set[String] = getRecentSegs(query)
      val recentList: Set[Int] = recentSegs.flatMap (x =>
        model.segStringIntMap.get(x))

      val recentFeatures: Vector[Array[Double]] = recentList.toVector
        // segModels may not contain the requested seg
        .map { i =>
          segModels.get(i).flatMap { sm => sm.features }
        }.flatten

      if (recentFeatures.isEmpty) {
        logger.info(s"No features vector for recent seg ${recentSegs}.")
        predictDefault(
          segModels = segModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList
        )
      } else {
        predictSimilar(
          recentFeatures = recentFeatures,
          segModels = segModels,
          query = query,
          whiteList = whiteList,
          blackList = finalBlackList
        )
      }
    }

    val segScores = topScores.map { case (si, s) =>
      new SegScore(
        // convert seg int index back to string ID
        seg = model.segIntStringMap(si),
        score = s
      )
    }

    new PredictedResult(segScores)
  }

  /** Generate final blackList based on other constraints */
  def genBlackList(query: Query): Set[String] = {
    // if unseenOnly is True, get all seen segs
    val seenSegs: Set[String] = if (ap.unseenOnly) {

      // get all user seg events which are considered as "seen" events
      val seenEvents: Iterator[Event] = try {
        LEventStore.findByEntity(
          appName = ap.appName,
          entityType = "user",
          entityId = query.user,
          eventNames = Some(ap.seenEvents),
          targetEntityType = Some(Some("seg")),
          // set time limit to avoid super long DB access
          timeout = Duration(200, "millis")
        )
      } catch {
        case e: scala.concurrent.TimeoutException =>
          logger.error(s"Timeout when read seen events." +
            s" Empty list is used. ${e}")
          Iterator[Event]()
        case e: Exception =>
          logger.error(s"Error when read seen events: ${e}")
          throw e
      }

      seenEvents.map { event =>
        try {
          event.targetEntityId.get
        } catch {
          case e => {
            logger.error(s"Can't get targetEntityId of event ${event}.")
            throw e
          }
        }
      }.toSet
    } else {
      Set[String]()
    }

    // get the latest constraint unavailableSegs $set event
    val unavailableSegs: Set[String] = try {
      val constr = LEventStore.findByEntity(
        appName = ap.appName,
        entityType = "constraint",
        entityId = "unavailableSegs",
        eventNames = Some(Seq("$set")),
        limit = Some(1),
        latest = true,
        timeout = Duration(200, "millis")
      )
      if (constr.hasNext) {
        constr.next.properties.get[Set[String]]("segs")
      } else {
        Set[String]()
      }
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read set unavailableSegs event." +
          s" Empty list is used. ${e}")
        Set[String]()
      case e: Exception =>
        logger.error(s"Error when read set unavailableSegs event: ${e}")
        throw e
    }

    // combine query's blackList,seenSegs and unavailableSegs
    // into final blackList.
    query.blackList.getOrElse(Set[String]()) ++ seenSegs ++ unavailableSegs
  }

  /** Get recent events of the user on segs for similar segs */
  def getRecentSegs(query: Query): Set[String] = {
    // get latest 10 user view seg events
    val recentEvents = try {
      LEventStore.findByEntity(
        appName = ap.appName,
        // entityType and entityId is specified for fast lookup
        entityType = "user",
        entityId = query.user,
        eventNames = Some(ap.similarEvents),
        targetEntityType = Some(Some("seg")),
        limit = Some(10),
        latest = true,
        // set time limit to avoid super long DB access
        timeout = Duration(200, "millis")
      )
    } catch {
      case e: scala.concurrent.TimeoutException =>
        logger.error(s"Timeout when read recent events." +
          s" Empty list is used. ${e}")
        Iterator[Event]()
      case e: Exception =>
        logger.error(s"Error when read recent events: ${e}")
        throw e
    }

    val recentSegs: Set[String] = recentEvents.map { event =>
      try {
        event.targetEntityId.get
      } catch {
        case e => {
          logger.error("Can't get targetEntityId of event ${event}.")
          throw e
        }
      }
    }.toSet

    recentSegs
  }

  /** Prediction for user with known feature vector */
  def predictKnownUser(
    userFeature: Array[Double],
    segModels: Map[Int, SegModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = segModels.par // convert to parallel collection
      .filter { case (i, sm) =>
        sm.features.isDefined &&
        isCandidateSeg(
          i = i,
          seg = sm.seg,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, sm) =>
        // NOTE: features must be defined, so can call .get
        val s = dotProduct(userFeature, sm.features.get)
        // may customize here to further adjust score
        (i, s)
      }
      .filter(_._2 > 0) // only keep segs with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Default prediction when know nothing about the user */
  def predictDefault(
    segModels: Map[Int, SegModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = segModels.par // convert back to sequential collection
      .filter { case (i, sm) =>
        isCandidateSeg(
          i = i,
          seg = sm.seg,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, sm) =>
        // may customize here to further adjust score
        (i, sm.count.toDouble)
      }
      .seq

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  /** Return top similar segs based on segs user recently has action on */
  def predictSimilar(
    recentFeatures: Vector[Array[Double]],
    segModels: Map[Int, SegModel],
    query: Query,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Array[(Int, Double)] = {
    val indexScores: Map[Int, Double] = segModels.par // convert to parallel collection
      .filter { case (i, sm) =>
        sm.features.isDefined &&
        isCandidateSeg(
          i = i,
          seg = sm.seg,
          whiteList = whiteList,
          blackList = blackList
        )
      }
      .map { case (i, sm) =>
        val s = recentFeatures.map{ rf =>
          // sm.features must be defined because of filter logic above
          cosine(rf, sm.features.get)
        }.reduce(_ + _)
        // may customize here to further adjust score
        (i, s)
      }
      .filter(_._2 > 0) // keep segs with score > 0
      .seq // convert back to sequential collection

    val ord = Ordering.by[(Int, Double), Double](_._2).reverse
    val topScores = getTopN(indexScores, query.num)(ord).toArray

    topScores
  }

  private
  def getTopN[T](s: Iterable[T], n: Int)(implicit ord: Ordering[T]): Seq[T] = {

    val q = PriorityQueue()

    for (x <- s) {
      if (q.size < n)
        q.enqueue(x)
      else {
        // q is full
        if (ord.compare(x, q.head) < 0) {
          q.dequeue()
          q.enqueue(x)
        }
      }
    }

    q.dequeueAll.toSeq.reverse
  }

  private
  def dotProduct(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var d: Double = 0
    while (i < size) {
      d += v1(i) * v2(i)
      i += 1
    }
    d
  }

  private
  def cosine(v1: Array[Double], v2: Array[Double]): Double = {
    val size = v1.size
    var i = 0
    var n1: Double = 0
    var n2: Double = 0
    var d: Double = 0
    while (i < size) {
      n1 += v1(i) * v1(i)
      n2 += v2(i) * v2(i)
      d += v1(i) * v2(i)
      i += 1
    }
    val n1n2 = (math.sqrt(n1) * math.sqrt(n2))
    if (n1n2 == 0) 0 else (d / n1n2)
  }

  private
  def isCandidateSeg(
    i: Int,
    seg: Seg,
    whiteList: Option[Set[Int]],
    blackList: Set[Int]
  ): Boolean = {
    // can add other custom filtering here
    whiteList.map(_.contains(i)).getOrElse(true) &&
    !blackList.contains(i)
  }

}
