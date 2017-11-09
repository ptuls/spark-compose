package com.compose.ds.ops

import com.compose.ds.ops.Context.DataSet
import org.apache.spark.sql.Dataset

object Context {
  type DataSet[A] = Dataset[A]
  type Weights[A] = Array[A]
}

sealed trait ModelF[A]
object Model {
  case class Fit[A](dataset: DataSet[_]) extends ModelF[A]
  case class Load[A](weights: Array[_]) extends ModelF[A]
  case class Transform[A](dataset: DataSet[_]) extends ModelF[A]
}

sealed trait TransformerF[A]
object Transformer {
  case class Transform[A](dataset: DataSet[_]) extends TransformerF[A]
}
