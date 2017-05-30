package hydra.jdbc.transport

import hydra.core.protocol.Produce
import hydra.core.transport.Transport

/**
  * Created by alexsilva on 5/20/17.
  */
class JdbcTransport extends Transport {

  transport {
    case Produce(r: JdbcRecord) =>
      println(s"JDBC RECORD $r")
  }
}
