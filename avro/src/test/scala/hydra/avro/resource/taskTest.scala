package hydra.avro.resource

import cats.Monad
import cats.data.NonEmptyList
import cats.effect._
import cats.implicits._
import org.scalatest.flatspec.AnyFlatSpecLike
import org.scalatest.matchers.should.Matchers

import scala.language.higherKinds

class taskTest extends Matchers with AnyFlatSpecLike {


  Data("qweASD").print[UpperCase] // QWEASD
  Data("qweASD").print[LowerCase] // qweasd


  trait UpperCase {
    implicit val instance: Formatter[UpperCase] = toUpper
  }
  def toUpper(s: String) = s.toUpperCase
  def toLower(s: String) = s.toLowerCase

//  object UpperCase extends UpperCase

  case class Data(x: String) {
    def print[A](implicit f: Formatter[A]) = println(f.format(x))
  }

//  trait UpperCase
  object UpperCase {
    implicit val instance: Formatter[UpperCase] = toUpper
  }

  trait LowerCase
  object LowerCase {
    implicit val instance: Formatter[LowerCase] = toLower
  }

  trait Formatter[X] {
    def format(s: String): String
  }

//  def toUpper(s: String) = s.toUpperCase
//  def toLower(s: String) = s.toLowerCase


  def transform[F[_] : Monad, A, E](fs: List[A => F[Either[E, Unit]]]): A => F[Either[NonEmptyList[E], Unit]] = {
    val accFunction: A => F[Either[NonEmptyList[E], Unit]] = (a: A) => {
      val func: A => F[Either[NonEmptyList[E], Unit]] = (_: A) => Monad[F].pure(Right(()))
      func(a)
    }
    a: A => {
      val func = fs.foldLeft(accFunction) {
        case (acc, element) => a: A =>
          for {
            resultAcc <- acc.apply(a)
            resultS <- element(a)
            k = (resultAcc, resultS) match {
              case (Left(list), Left(err)) => Left(err :: list)
              case (ab@Left(_), Right(_)) => ab
              case (Right(_), Left(err)) => Left(NonEmptyList.one(err))
              case _ => Right(())
            }
          } yield k
      }
      func(a)
    }

  }

  def transform1[F[_] : Monad, A, E](fs: List[A => F[Either[E, Unit]]]): A => F[Either[NonEmptyList[E], Unit]] = {
    a: A => fs.traverse(_ (a)).map(_.separate._1).map(NonEmptyList.fromList(_).toLeft(()))
  }

  def transform2[F[_] : Monad, A, E](fs: List[A => F[Either[E, Unit]]]): A => F[Either[NonEmptyList[E], Unit]] = {
    a: A => fs.traverse(_ (a)).map(_.foldMap(_.toValidatedNel).toEither)
  }

  def pack[A](xs: Seq[A]): Seq[(A, Int)] = xs.foldLeft(Seq[(A, Int)]()) {
    case (head :: tail, value) if head._1 == value => (value, head._2 + 1) +: tail
    case (acc, value) => (value, 1) +: acc
  }.toList.reverse


  it should "pack the list" in {
    pack(Seq(1, 2, 2, 3, 4, 3, 3, 3)) shouldBe Seq((1, 1), (2, 2), (3, 1), (4, 1), (3, 3))
  }


  it should "fff" in {
    val fs: List[Int => IO[Either[String, Unit]]] = List(
      { a => IO(if (a % 2 == 0) Right(()) else Left("a doesnt fit 2")) },
      { a => IO(if (a % 3 == 2) Right(()) else Left("a doesnt fit 3")) },
      { a => IO(if (a % 5 == 3) Right(()) else Left("a doesnt fit 4")) }
    )
    val funcs = transform2(fs)

    val res = funcs(8).unsafeRunSync()
    res shouldBe Right(())
  }

  it should "transform to left" in {

    val fs: List[Int => IO[Either[String, Unit]]] = List(
      { a => IO(if (a % 2 == 0) Right(()) else Left("a doesnt fit 2")) },
      { a => IO(if (a % 3 == 0) Right(()) else Left("a doesnt fit 3")) },
      { a => IO(if (a % 5 == 0) Right(()) else Left("a doesnt fit 4")) }
    )
    val funcs = transform2(fs)

    val res = funcs(8).unsafeRunSync()
    res shouldBe Left(NonEmptyList("a doesnt fit 3", List("a doesnt fit 4")))

  }

}
