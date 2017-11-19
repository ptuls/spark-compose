package com.compose.spark.ops

import com.compose.spark.error.{FileReadError, SparkError}
import org.apache.spark.sql._

import scalaz._

object ReadOps {
  def readCsv(fileName: String,
               sess: SparkSession): SparkError \/ Dataset[Row] = {
    \/.fromTryCatchNonFatal(sess.read.csv(fileName))
      .leftMap[SparkError](e => FileReadError(e.getMessage))
  }

  def readParquet(fileName: String,
                  sess: SparkSession): SparkError \/ Dataset[Row] = {
    \/.fromTryCatchNonFatal(sess.read.parquet(fileName))
      .leftMap[SparkError](e => FileReadError(e.getMessage))
  }

  def readJson(fileName: String,
               sess: SparkSession): SparkError \/ Dataset[Row] = {
    \/.fromTryCatchNonFatal(sess.read.json(fileName)).leftMap[SparkError](e =>
      FileReadError(e.getMessage))
  }

  def readText(fileName: String,
               sess: SparkSession): SparkError \/ Dataset[String] = {
    \/.fromTryCatchNonFatal(sess.read.textFile(fileName))
      .leftMap[SparkError](e => FileReadError(e.getMessage))
  }

  def readLibSVM(fileName: String,
                 sess: SparkSession): SparkError \/ Dataset[Row] = {
    \/.fromTryCatchNonFatal(sess.read.format("libsvm").load(fileName))
      .leftMap[SparkError](e => FileReadError(e.getMessage))
  }
}
