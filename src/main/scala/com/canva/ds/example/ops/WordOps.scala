package com.canva.ds.example.ops

import com.canva.ds.example.core.SparkAction
import org.apache.spark.sql.{Dataset, Row}
import org.apache.spark.sql.functions._

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
  def countOp: SparkAction[Dataset[Row]] =
    for (words <- wordsOp)
      yield
        words
          .groupBy("value")
          .agg(count("*") as "frequency")
          .orderBy(col("frequency").desc)

  def topWordsOp(n: Int): SparkAction[Dataset[Row]] =
    countOp.map(_.limit(n))
}
