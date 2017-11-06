package com.compose.ds.example

import com.compose.ds.core.SparkAction
import com.compose.ds.error.{FileReadError, SessionCreateError, SparkError}
import com.compose.ds.ops.SparkOps
import com.typesafe.scalalogging.LazyLogging
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Encoders}

import scalaz.{-\/, \/-}

case class WordFrequency(word: String, frequency: Long)

class WordOps(textFilePath: String) {
  // initial SparkOperation created using companion object
  def linesOp: SparkAction[Dataset[String]] = SparkAction { sess =>
    SparkOps.readText(textFilePath, sess)
  }

  // after that we often just need map / flatMap
  def wordsOp: SparkAction[Dataset[String]] =
    for (lines <- linesOp) yield {
      import lines.sparkSession.implicits._
      lines
        .flatMap { line =>
          line.split("\\W+")
        }
        .map(_.toLowerCase)
        .filter(!_.isEmpty)
    }

  // now get distinct words with count
  def countOp: SparkAction[Dataset[WordFrequency]] =
    for (words <- wordsOp)
      yield
        words
          .groupBy("value")
          .agg(count("*") as "frequency")
          .orderBy(col("frequency").desc)
          .withColumnRenamed("value", "word")
          .as[WordFrequency](Encoders.product)

  def topWordsOp(n: Int): SparkAction[Dataset[WordFrequency]] =
    countOp.map(_.limit(n))
}


object WordCountMain extends LazyLogging {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val path = "src/main/resources/test.txt"

    /* resource setup is separated from computation */
    val conf = new SparkConf().setMaster("local[2]").setAppName("Word count example")
    val sparkSession = SparkOps.initSparkSession(conf)

    logger.info("Running word count...")
    val topWordsMap = sparkSession.flatMap(sess => new WordOps(path).topWordsOp(10).run(sess))
    topWordsMap match {
      case \/-(ds)  => ds.show()
      case -\/(e)   => logger.error(renderError(e))
    }
    logger.info("Completed")

    /* teardown resources */
    sparkSession.map(_.stop())
  }

  def renderError(error: SparkError): String = {
    error match {
      case FileReadError(e) =>
        s"Error reading file ${e.toString}; does it exist?"
      case SessionCreateError(e) =>
        s"Error creating Spark session: ${e.toString}"
      case _ => "Unknown error"
    }
  }
}