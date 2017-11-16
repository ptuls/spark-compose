package com.compose.spark.core

import com.compose.spark.error.SparkError
import org.apache.spark.sql.SparkSession

import scalaz._

sealed trait SparkAction[+A] {
  def run(session: SparkSession): SparkError \/ A

  def map[B](f: A => B): SparkAction[B] =
    SparkAction(session => run(session).map(f))
  def flatMap[B](f: A => SparkAction[B]): SparkAction[B] =
    SparkAction(session => run(session).flatMap(a => f(a).run(session)))
}

object SparkAction {
  def apply[A](f: SparkSession => SparkError \/ A): SparkAction[A] =
    new SparkAction[A] {
      override def run(session: SparkSession): SparkError \/ A =
        f(session)
    }

  def ok[A](value: A): SparkAction[A] = SparkAction(_ => \/.right(value))

  def fail[A](error: SparkError): SparkAction[A] =
    SparkAction(_ => \/.left(error))

  private def tryCatch[A](f: => A): SparkError \/ A =
    try { \/.right(f) } catch { case e: SparkError => \/.left(e) }

  implicit def SparkActionMonad: Monad[SparkAction] =
    new Monad[SparkAction] {
      override def point[A](v: => A): SparkAction[A] = ok[A](v)
      override def bind[A, B](m: SparkAction[A])(
          f: A => SparkAction[B]): SparkAction[B] = m.flatMap(f)
    }
}

trait SparkPlugin {
  def apply(input: Option[Any]): SparkAction[Any]
}
