package hydra.common.testing

import scala.concurrent.duration._

/**
  * Created by alexsilva on 3/6/17.
  */
case class TestCase(name: String, value: Int, duration: Duration = 1.second)
