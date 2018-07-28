package com.compose.spark.example

import scala.language.{higherKinds, reflectiveCalls}
import scalaz.{Free, FreeAp, Inject, NaturalTransformation}

case class User(name: String, age: Int)

sealed trait UserOperation[T]
case class CreateUser(name: String, age: Int) extends UserOperation[User]

sealed trait AnalyticsOperation[T]
case class AnalyseUser(user: User) extends AnalyticsOperation[Int]

case class ExecStrategy[F[_], A](fa: F[A]) {
  val seq: Free[F, A] = Free.liftF(fa)
  val par: FreeAp[F, A] = FreeAp.lift(fa)
}

case class UserRepo[F[_]](implicit ev: Inject[UserOperation, F]) {
  def createUser(name: String, age: Int): ExecStrategy[F, User] =
    ExecStrategy[F, User](ev.inj(CreateUser(name, age)))
}

object UserRepo {
  implicit def toUserRepo[F[_]](
      implicit ev: Inject[UserOperation, F]): UserRepo[F] = new UserRepo[F]()
}

case class AnalyticsRepo[F[_]](implicit ev: Inject[AnalyticsOperation, F]) {
  def analyseUser(user: User): ExecStrategy[F, Int] =
    ExecStrategy[F, Int](ev.inj(AnalyseUser(user)))
}

object AnalyticsRepo {
  implicit def toAnalyticsRepo[F[_]](
      implicit ev: Inject[AnalyticsOperation, F]): AnalyticsRepo[F] =
    new AnalyticsRepo[F]()
}

//object ProgramHelpers {
//  type Program[F[_], A] = Free[FreeAp[F, ?], A]
//
//  implicit class RichFree[F[_], A](free: Free[F, A]) {
//    def asProgramStep: Program[F, A] = {
//      free.foldMap[Program[F, ?]](new NaturalTransformation[F, Program[F, ?]] {
//        override def apply[A](fa: F[A]): Program[F, A] = liftFA(fa)
//      })(Free.freeMonad[FreeAp[F, ?]])
//    }
//  }
//
//  implicit class RichFreeAp[F[_], A](freeap: FreeAp[F, A]) {
//    def asProgramStep: Program[F, A] = Free.liftF[FreeAp[F, ?], A](freeap)
//  }
//
//  def liftFA[F[_], A](fa: F[A]): Program[F, A] =
//    Free.liftF[FreeAp[F, ?], A](FreeAp.lift(fa))
//}
