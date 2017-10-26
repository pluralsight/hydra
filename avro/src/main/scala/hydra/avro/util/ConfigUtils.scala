package hydra.avro.util

import java.util.Properties

import com.typesafe.config._

import scala.collection.JavaConverters._
import scala.collection._

/**
  * Created by alexsilva on 7/13/17.
  */
object ConfigUtils {

  implicit def toProperties(cfg: Config): Properties = {
    val props = new Properties()
    val map: Map[String, Object] = cfg.entrySet().asScala.map({ entry =>
      entry.getKey -> entry.getValue.unwrapped()
    })(collection.breakOut)

    props.putAll(map.asJava)
    props
  }

}
