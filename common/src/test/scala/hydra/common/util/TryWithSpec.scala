package hydra.common.util

import java.io.Closeable

import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpecLike

import scala.util.{Failure, Success}

class TryWithSpec extends AnyWordSpecLike with Matchers {
  // Exceptions and errors here so we don't pay the stack trace creation cost multiple times
  val getResourceException = new RuntimeException
  val inFunctionException = new RuntimeException
  val inCloseException = new RuntimeException
  val getResourceError = new OutOfMemoryError
  val inFunctionError = new OutOfMemoryError
  val inCloseError = new OutOfMemoryError

  val goodResource = new Closeable {
    override def toString: String = "good resource"
    def close(): Unit = {}
  }

  "TryWith" should {
    "catch exceptions getting the resource" in {
      TryWith(throw getResourceException)(println) shouldBe Failure(
        getResourceException
      )
    }

    "catch exceptions in the function" in {
      TryWith(goodResource) { _ =>
        throw inFunctionException
      } shouldBe Failure(inFunctionException)
    }

    "catch exceptions while closing" in {
      TryWith(new Closeable {
        def close(): Unit = throw inCloseException
      })(_.toString) shouldBe Failure(inCloseException)
    }

    "note suppressed exceptions" in {
      val ex = new RuntimeException
      val result = TryWith(new Closeable {
        def close(): Unit = throw inCloseException
      })(_ => throw ex)

      result shouldBe Failure(ex)
      val Failure(returnedException) = result
      returnedException.getSuppressed shouldBe Array(inCloseException)
    }

    "propagate errors getting the resource" in {
      intercept[OutOfMemoryError] {
        TryWith(throw getResourceError)(println)
      } shouldBe getResourceError
    }

    "propagate errors in the function" in {
      intercept[OutOfMemoryError] {
        TryWith(goodResource) { _ => throw inFunctionError }
      } shouldBe inFunctionError
    }

    "propagate errors while closing" in {
      intercept[OutOfMemoryError] {
        TryWith(new Closeable {
          def close(): Unit = throw inCloseError
        })(_.toString)
      } shouldBe inCloseError
    }

    "return the value from a successful run" in {
      TryWith(goodResource)(_.toString) shouldBe Success("good resource")
    }
  }
}
