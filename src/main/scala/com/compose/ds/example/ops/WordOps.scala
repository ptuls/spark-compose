package com.compose.ds.example.ops

import com.compose.ds.example.core.SparkAction
import org.apache.spark.sql.{Dataset, Encoders}
import org.apache.spark.sql.functions._

case class WordFrequency(word: String, frequency: Long)

object WordOps {
  // initial SparkOperation created using companion object
  def linesOp: SparkAction[Dataset[String]] = SparkAction { sess =>
    SparkOps.readText("src/main/resources/test.txt", sess)
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
