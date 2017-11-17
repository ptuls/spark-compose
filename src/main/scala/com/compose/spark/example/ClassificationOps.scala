package com.compose.spark.example

import com.compose.spark.core.SparkAction
import com.compose.spark.error.ErrorHandler.renderError
import com.compose.spark.ops.{ReadOps, SparkOps}
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.{Dataset, Row}

import scalaz.{-\/, \/-}

class ClassificationOps(trainDatasetPath: String, predictDatasetPath: String) {
  def loadDatasetOp(datasetPath: String): SparkAction[Dataset[Row]] =
    SparkAction(sess => ReadOps.readLibSVM(datasetPath, sess))

  def whitenDatasetOp(datasetPath: String): SparkAction[Dataset[Row]] =
    for (dataset <- loadDatasetOp(datasetPath)) yield whitenDataset(dataset)

  def trainingOp: SparkAction[LogisticRegressionModel] =
    for (whitenedDataset <- whitenDatasetOp(trainDatasetPath))
      yield new LogisticRegression().setRegParam(0.0).fit(whitenedDataset)

  def predictOp: SparkAction[Dataset[Row]] =
    for {
      model <- trainingOp
      whitenedPredictionDataset <- whitenDatasetOp(predictDatasetPath)
    } yield model.transform(whitenedPredictionDataset)

  def modelScoringOp: SparkAction[Map[String, Double]] =
    for {
      predictions <- predictOp
    } yield {
      val metricList = List("accuracy", "f1", "weightedPrecision", "weightedRecall")
      val metricScores = metricList.map(metric => computeMetric(predictions, metric))
      metricList.zip(metricScores).toMap
    }

  def whitenDataset(dataset: Dataset[Row]): Dataset[Row] = {
    val stdScaler = new StandardScaler()
      .setWithMean(true)
      .setWithStd(true)
      .setInputCol("features")
      .setOutputCol("whitenedFeatures")
    val stdScalerModel = stdScaler.fit(dataset)
    val transformedDataset = stdScalerModel.transform(dataset)
    transformedDataset
      .drop("features")
      .withColumnRenamed("whitenedFeatures", "features")
  }

  def computeMetric(dataset: Dataset[Row], metricName: String): Double = {
    val multiclassClassificationEvaluator =
      new MulticlassClassificationEvaluator()
        .setPredictionCol("prediction")
        .setLabelCol("label")
        .setMetricName(metricName)
    multiclassClassificationEvaluator.evaluate(dataset)
  }

}

object ClassificationExampleMain extends LazyLogging {
  private val basePath: String = "src/main/resources/classification/"

  def main(args: Array[String]): Unit = {
    /* resource setup is separated from computation */
    val conf =
      new SparkConf()
        .setMaster("local[4]")
        .setAppName("Multiclass classification example")
    val sparkSession = SparkOps.initSparkSession(conf)

    val trainingSetPath = basePath + "sample_multiclass_training_data.txt"
    val predictionSetPath = basePath + "sample_multiclass_prediction_data.txt"
    val scores = sparkSession.flatMap(
      sess =>
        new ClassificationOps(trainingSetPath, predictionSetPath).modelScoringOp
          .run(sess)
    )
    scores match {
      case \/-(s) => renderScores(s)
      case -\/(e) => logger.error(renderError(e))
    }
    sparkSession.map(_.stop())
  }

  def renderScores(scoreMap: Map[String, Double]): Unit =
    scoreMap.toList.foreach {
      case (metric, score) => logger.info(s"$metric: ${score * 100.0}%")
    }

}
