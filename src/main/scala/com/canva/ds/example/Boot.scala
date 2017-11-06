package com.canva.ds.example

import com.canva.ds.example.error.{FileReadError, SessionCreateError, SparkError}
import com.canva.ds.example.ops.WordOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import scalaz.{-\/, \/-}

object Boot extends LazyLogging {
  def main(args: Array[String]): Unit = {
    /* resource setup is separated from computation */
    val conf = new SparkConf().setMaster("local[2]").setAppName("Word count example")
    val spark = SparkSession.builder().config(conf).getOrCreate()

    logger.info("Running word count...")
    val topWordsMap = WordOps.topWordsOp(10).run(spark)
    topWordsMap match {
      case \/-(ds)  => ds.show()
      case -\/(e)   => logger.error(renderError(e))
    }
    logger.info("Completed")

    /* teardown resources */
    spark.stop()
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
