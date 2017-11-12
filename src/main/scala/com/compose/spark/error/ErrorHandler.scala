package com.compose.spark.error

object ErrorHandler {
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
