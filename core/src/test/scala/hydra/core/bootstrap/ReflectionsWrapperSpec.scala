package hydra.core.bootstrap

import org.scalatest.BeforeAndAfterAll
import org.scalatest.matchers.should.Matchers
import org.scalatest.flatspec.AnyFlatSpecLike
import test.scan.TestServiceProvider

/**
  * Created by alexsilva on 3/7/17.
  */
class ReflectionsWrapperSpec
    extends Matchers
    with AnyFlatSpecLike
    with BeforeAndAfterAll {

  "The ReflectionsWrapper object" should "load package list from configs in" in {
    //scan-packages
    ReflectionsWrapper.scanPkgs should contain allOf ("hydra", "test.scan")
  }

  it should "load by subtype" in {
    ReflectionsWrapper.reflections.getSubTypesOf(classOf[ServiceProvider]) should contain(
      classOf[TestServiceProvider]
    )
  }
}
