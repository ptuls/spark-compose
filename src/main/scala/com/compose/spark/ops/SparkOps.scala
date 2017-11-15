package com.compose.spark.ops

import com.compose.spark.error.{SessionCreateError, SparkError}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scalaz.\/

object SparkOps {
  def initSparkSession(conf: SparkConf): SparkError \/ SparkSession = {
    \/.fromTryCatchNonFatal(SparkSession.builder().config(conf).getOrCreate())
      .leftMap[SparkError](e => SessionCreateError(e.getMessage))
  }
}
