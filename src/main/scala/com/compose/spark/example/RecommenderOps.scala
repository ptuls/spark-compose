package com.compose.spark.example

import com.compose.spark.core.SparkAction
import com.compose.spark.error.ErrorHandler.renderError
import com.compose.spark.ops.{ReadOps, SparkOps}
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.ml.evaluation.RegressionEvaluator
import org.apache.spark.ml.recommendation.{ALS, ALSModel}
import org.apache.spark.sql.{Dataset, Encoders, Row}

import scalaz.{-\/, \/-}

case class Rating(userId: Int, movieId: Int, rating: Float, timestamp: Long)

class RecommenderOps(datasetPath: String) extends Serializable {
  def loadDatasetOp(datasetPath: String): SparkAction[Dataset[String]] =
    SparkAction(sess => ReadOps.readText(datasetPath, sess))

  def parseDatasetOp: SparkAction[Dataset[Rating]] =
    for (rawDataset <- loadDatasetOp(datasetPath))
      yield {
        import rawDataset.sparkSession.implicits._
        rawDataset.map(parseRating).as[Rating](Encoders.product)
      }

  def splitDatasetOp(splitParam: Double): SparkAction[Array[Dataset[Rating]]] =
    for (ratings <- parseDatasetOp)
      yield ratings.randomSplit(Array(splitParam, 1.0 - splitParam))

  def trainingAndTestingOp: SparkAction[Dataset[Row]] =
    for {
      splitArray <- splitDatasetOp(0.8)
      Array(trainDataset, testDataset) = splitArray
    } yield {
      val avgRating = getAvgRating(trainDataset)
      val model = trainingOp(trainDataset)
      testingOp(model, testDataset, avgRating)
    }

  def evaluationOp: SparkAction[Double] =
    for (predictions <- trainingAndTestingOp) yield {
      val evaluator = new RegressionEvaluator()
        .setMetricName("rmse")
        .setLabelCol("rating")
        .setPredictionCol("prediction")
      evaluator.evaluate(predictions)
    }

  /* non-negative matrix factorization collaborative filtering */
  def trainingOp(trainingDataset: Dataset[Rating]): ALSModel = {
    val als = new ALS()
      .setMaxIter(5)
      .setRegParam(0.01)
      .setUserCol("userId")
      .setItemCol("movieId")
      .setRatingCol("rating")
      .setNonnegative(true)
    als.fit(trainingDataset)
  }

  def testingOp(model: ALSModel,
                testDataset: Dataset[Rating],
                avgRating: Double): Dataset[Row] = {
    val rawPredictions = model.transform(testDataset)
    rawPredictions.na.fill(avgRating)
  }

  def parseRating(str: String): Rating = {
    val fields = str.split(",")
    assert(fields.size == 4)
    Rating(fields(0).toInt,
           fields(1).toInt,
           fields(2).toFloat,
           fields(3).toLong)
  }

  def getAvgRating(dataset: Dataset[Rating]): Double =
    dataset
      .select("rating")
      .groupBy("rating")
      .avg("rating")
      .first
      .getAs[Float](0)
      .toDouble
}

object RecommenderExampleMain extends LazyLogging {
  private val basePath: String = "src/main/resources/recommender/"

  def main(args: Array[String]): Unit = {
    /* resource setup is separated from computation */
    val conf =
      new SparkConf()
        .setMaster("local[*]")
        .setAppName("Basic recommender")
    val sparkSession = SparkOps.initSparkSession(conf)

    val datasetPath = basePath + "sample_movielens_rating.txt"
    val rmseScore = sparkSession.flatMap(sess =>
      new RecommenderOps(datasetPath).evaluationOp.run(sess))
    rmseScore match {
      case \/-(s) => logger.info("RMSE: {}", s)
      case -\/(e) => logger.error(renderError(e))
    }
    sparkSession.map(_.stop())
  }

}
