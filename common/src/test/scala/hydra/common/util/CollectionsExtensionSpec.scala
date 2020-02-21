package hydra.common.util

import java.util.Properties

import org.scalatest.{FunSpecLike, Matchers}

/**
  * Created by alexsilva on 3/6/17.
  */
class CollectionsExtensionSpec extends Matchers with FunSpecLike {
  describe("When using collection extensions") {
    it("converts camel to underscore case") {
      import CollectionExtensions._
      val m: Map[String, AnyRef] = Map(
        "test" -> "value",
        "no" -> new Integer(1),
        "nod" -> "2.0",
        "dt" -> org.joda.time.DateTime.now()
      )
      val p = new Properties()
      m.foreach(v => p.put(v._1, v._2.toString))

      val implicitProps: Properties = m

      implicitProps shouldBe p
    }
  }

}
