package hydra.common.config

import com.typesafe.config.Config

/**
  * Created by alexsilva on 3/2/17.
  */
trait ConfigComponent {
  val rootConfig: Config

  val applicationName: String
}
