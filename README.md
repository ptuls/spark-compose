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

## Recommender Pipeline Example

In this example, a non-negative matrix factorization, collaborative filtering-based recommender system is applied on the [Movielens](https://grouplens.org/datasets/movielens/100k/) dataset. This includes scoring for the model at the end of the pipeline (in terms of root mean squared error).

To run the example, run
```
sbt "runMain com.compose.ds.example.RecommenderExampleMain"
```
