package com.compose.spark.ops

import com.compose.spark.ops.Context.{DataSet, State}
import org.apache.spark.sql.Dataset

object Context {
  type DataSet[A] = Dataset[A]
  type State[A] = Dataset[A]
}

/**
  *
  * Definition:
  * 1. in fitting, a model takes in a dataset and produces a model state
  * 2. in transforming, a model takes a dataset and a model state and produces a dataset
  *
  */
sealed trait ModelF[A]
object Model {
  case class Fit[A](dataset: DataSet[_]) extends ModelF[A]
  case class Transform[A](dataset: DataSet[_], state: State[A]) extends ModelF[A]
}

/**
  * A transformer is simply a pure function that takes in a dataset and produces a dataset.
  */
sealed trait TransformerF[A]
object Transformer {
  case class Transform[A](dataset: DataSet[_]) extends TransformerF[A]
}
