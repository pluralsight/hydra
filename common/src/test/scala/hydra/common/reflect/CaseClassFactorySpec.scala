package hydra.common.reflect

import hydra.common.testing.{DummyActor, TestCase}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/3/17.
  */
class CaseClassFactorySpec extends Matchers with AnyFunSpecLike {
  describe("When using ReflectionUtils") {
    it("Instantiates a case class with constructor params") {
      new CaseClassFactory(classOf[TestCase])
        .buildWith(Seq("name", 120, 2.seconds)) shouldBe
        TestCase("name", 120, 2 seconds)
    }

    it("Rejects non-case classes") {
      intercept[IllegalArgumentException] {
        new CaseClassFactory(classOf[DummyActor]).buildWith(Seq("name", 120))
      }
    }
  }

}
