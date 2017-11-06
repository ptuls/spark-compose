# Spark Compose

Composable pipelines in Apache Spark. Inspired by [Spark-Plug](https://github.com/springnz/sparkplug).

## Word Count Example

To run the word count example, in the console, run
```
sbt run
```
as the word count example is set as the main class.

## Classification Pipeline Example

In this example, a logistic regression model is fitted to a set of whitened feature vectors before being used to make predictions on a test dataset.

To run the example, run
```
sbt "runMain com.compose.ds.example.ClassificationExampleMain"
```
