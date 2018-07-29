package com.compose.spark.example

import breeze.stats.distributions.{Bernoulli, Beta}

import scala.annotation.tailrec
import scalaz.Scalaz._
import scalaz._

sealed trait Counter
case class BanditCounter(successCount: Int, failureCount: Int) extends Counter

object ThompsonSamplingExampleMain {
  def main(args: Array[String]): Unit = {
    val bernoulliDistParams = Array(0.1, 0.6, 0.3)
    val bernoulliDists: Array[Bernoulli] =
      bernoulliDistParams.map(Bernoulli.distribution)

    /**
      * MaxEnt-style prior distribution, corresponds to Beta(1,1), i.e., uniform distribution
      */
    val initState =
      Array(BanditCounter(1, 1), BanditCounter(1, 1), BanditCounter(1, 1))

    println("Running simulation")
    val finalState = simulate(1000)(initState, bernoulliDists, pullThompson())
    finalState.zipWithIndex.foreach {
      case (banditCounter: BanditCounter, index: Int) =>
        println(s"Bandit counter $index: ${banditCounter.successCount.toDouble /
          (banditCounter.successCount.toDouble + banditCounter.failureCount.toDouble)}")
    }
  }

  @tailrec
  def simulate(trials: Int)(
      init: Array[BanditCounter],
      bernoulliDists: Array[Bernoulli],
      pullStrategy: State[Array[BanditCounter], Int]
  ): Array[BanditCounter] = {
    assert(trials >= 0, "number of trials must be non-negative")

    val pullState = pullThompson().run(init)
    val updateState = update(bernoulliDists, pullState._2).run(pullState._1)._1
    if (trials > 0) {
      simulate(trials - 1)(updateState, bernoulliDists, pullStrategy)
    } else {
      updateState
    }
  }

  def getReward(dists: Array[Bernoulli], chosenArm: Int): Boolean = {
    assert(chosenArm >= 0 && chosenArm < dists.length)
    dists(chosenArm).sample()
  }

  /***
    * Choose the arm to pull
    * @return state with action i.e., chosen arm
    */
  def pullThompson(): State[Array[BanditCounter], Int] = {
    for {
      currentState <- get[Array[BanditCounter]]
    } yield {
      val thetas = sample(currentState)
      thetas.zipWithIndex.maxBy(_._1)._2
    }
  }

  def pullGreedy(): State[Array[BanditCounter], Int] = {
    for {
      currentState <- get[Array[BanditCounter]]
    } yield {
      meanBetaDist(currentState).zipWithIndex.maxBy(_._1)._2
    }
  }

  /**
    * Update counters based on chosen arm
    * @param dists
    * @param chosenArm
    * @return
    */
  def update(dists: Array[Bernoulli],
             chosenArm: Int): State[Array[BanditCounter], Unit] = {
    for {
      nextState <- modify[Array[BanditCounter]](currentState =>
        updateState(currentState, chosenArm, getReward(dists, chosenArm)))
    } yield nextState
  }

  private def sample(counters: Array[BanditCounter]): Array[Double] = {
    val distributions = counters.map { c =>
      Beta.distribution((c.successCount.toDouble, c.failureCount.toDouble))
    }
    distributions.map(_.draw)
  }

  private def meanBetaDist(counters: Array[BanditCounter]): Array[Double] = {
    counters.map { c =>
      c.successCount.toDouble / (c.successCount + c.failureCount).toDouble
    }
  }

  private def updateState(counters: Array[BanditCounter],
                          chosenArm: Int,
                          reward: Boolean): Array[BanditCounter] = {
    val newBanditCounter = if (reward) {
      BanditCounter(counters(chosenArm).successCount + 1,
                    counters(chosenArm).failureCount)
    } else {
      BanditCounter(counters(chosenArm).successCount,
                    counters(chosenArm).failureCount + 1)
    }

    counters.zipWithIndex.map {
      case (banditCounter: BanditCounter, index: Int) =>
        if (index == chosenArm) {
          newBanditCounter
        } else {
          banditCounter
        }
    }
  }
}
