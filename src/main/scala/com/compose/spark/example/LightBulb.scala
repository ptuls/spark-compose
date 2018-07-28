package com.compose.spark.example

import scalaz._
import Scalaz._

sealed trait Switch
case object LightSwitch extends Switch

sealed trait LightStatus
case object On extends LightStatus
case object Off extends LightStatus

case class LightBulb(lightStatus: LightStatus) {
  override def toString: String = s"light bulb: ${this.lightStatus.toString}"
}

object LightBulbMain {
  def main(args: Array[String]): Unit = {
    println(flipSwitch(LightSwitch).run(LightBulb(Off))._1)
    println(flipSwitch(LightSwitch).run(LightBulb(On))._1)
  }

  def flipSwitch(s: Switch): State[LightBulb, Unit] = {
    for {
      currStatus <- get[LightBulb]
      newStatus <- if (currStatus.lightStatus == On) {
        put(currStatus.copy(lightStatus = Off))
      } else {
        put(currStatus.copy(lightStatus = On))
      }
    } yield newStatus
  }

}
