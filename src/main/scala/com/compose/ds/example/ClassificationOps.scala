package com.compose.ds.example

import com.compose.ds.core.SparkAction
import com.compose.ds.error.ErrorHandler.renderError
import com.compose.ds.ops.SparkOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.StandardScaler
import org.apache.spark.sql.{Dataset, Row}

import scalaz.{-\/, \/-}

class ClassificationOps(trainDatasetPath: String, predictDatasetPath: String) {
  def loadDatasetOp(datasetPath: String): SparkAction[Dataset[Row]] =
    SparkAction(sess => SparkOps.readLibSVM(datasetPath, sess))

  def whitenDatasetOp(datasetPath: String): SparkAction[Dataset[Row]] =
    for (dataset <- loadDatasetOp(datasetPath)) yield {
      val stdScaler = new StandardScaler()
        .setWithMean(true)
        .setWithStd(true)
        .setInputCol("features")
        .setOutputCol("whitenedFeatures")
      val stdScalerModel = stdScaler.fit(dataset)
      val transformedDataset = stdScalerModel.transform(dataset)
      transformedDataset.drop("features").withColumnRenamed("whitenedFeatures", "features")
    }

  def trainingOp: SparkAction[LogisticRegressionModel] =
    for (whitenedDataset <- whitenDatasetOp(trainDatasetPath))
      yield new LogisticRegression().setRegParam(0.0).fit(whitenedDataset)

  def predictOp: SparkAction[Dataset[Row]] =
    for {
      model <- trainingOp
      whitenedPredictionDataset <- whitenDatasetOp(predictDatasetPath)
    } yield model.transform(whitenedPredictionDataset)

}

object ClassificationExampleMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    /* resource setup is separated from computation */
    val conf =
      new SparkConf().setMaster("local[4]").setAppName("Multiclass classification example")
    val sparkSession = SparkOps.initSparkSession(conf)

    val trainingSetPath = "src/main/resources/sample_multiclass_training_data.txt"
    val predictionSetPath = "src/main/resources/sample_multiclass_prediction_data.txt"
    val predictions = sparkSession.flatMap(
      sess =>
        new ClassificationOps(trainingSetPath, predictionSetPath).predictOp
          .run(sess))
    predictions match {
      case \/-(p) => p.show()
      case -\/(e) => logger.error(renderError(e))
    }
    sparkSession.map(_.stop())
  }

}
