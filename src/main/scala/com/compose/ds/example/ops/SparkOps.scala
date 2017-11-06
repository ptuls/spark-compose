package com.compose.ds.example.ops

import com.compose.ds.example.error.{FileReadError, SessionCreateError, SparkError}
import org.apache.spark.SparkConf
import org.apache.spark.sql._

import scalaz._

object SparkOps {
  def initSparkSession(conf: SparkConf): SparkError \/ SparkSession = {
    \/.fromTryCatchNonFatal(SparkSession.builder().config(conf).getOrCreate())
      .leftMap[SparkError](e => SessionCreateError(e.getMessage))
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
}
