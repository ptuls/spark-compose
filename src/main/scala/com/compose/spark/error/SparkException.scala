package com.compose.spark.error

sealed trait SparkError
case class FileReadError(e: String) extends SparkError
case class SessionCreateError(e: String) extends SparkError
