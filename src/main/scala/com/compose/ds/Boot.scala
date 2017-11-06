package com.compose.ds

import com.compose.ds.error.{FileReadError, SessionCreateError, SparkError}
import com.compose.ds.example.WordOps
import com.compose.ds.ops.SparkOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf

import scalaz.{-\/, \/-}

object Boot extends LazyLogging {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    /* resource setup is separated from computation */
    val conf = new SparkConf().setMaster("local[2]").setAppName("Word count example")
    val sparkSession = SparkOps.initSparkSession(conf)

    logger.info("Running word count...")
    val topWordsMap = sparkSession.flatMap(sess => WordOps.topWordsOp(10).run(sess))
    topWordsMap match {
      case \/-(ds)  => ds.show()
      case -\/(e)   => logger.error(renderError(e))
    }
    logger.info("Completed")

    /* teardown resources */
    sparkSession.map(sess => sess.stop())
  }

  def renderError(error: SparkError): String = {
    error match {
      case FileReadError(e) =>
        s"Error reading file ${e.toString}; does it exist?"
      case SessionCreateError(e) =>
        s"Error creating Spark session: ${e.toString}"
    }
  }
}
