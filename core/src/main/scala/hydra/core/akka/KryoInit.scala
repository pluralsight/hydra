package hydra.core.akka

import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo._
import hydra.common.logging.LoggingAdapter

import scala.collection._

class KryoInit extends LoggingAdapter {

  def customize(kryo: Kryo): Unit = {
    log.debug("Initializing Kryo...")
    kryo.addDefaultSerializer(
      classOf[scala.Enumeration#Value],
      classOf[EnumerationSerializer]
    )
    kryo.addDefaultSerializer(
      classOf[mutable.HashMap[_, _]],
      classOf[ScalaMutableMapSerializer]
    )
    kryo.addDefaultSerializer(
      classOf[scala.collection.immutable.Map[_, _]],
      classOf[ScalaImmutableAbstractMapSerializer]
    )
    kryo.register(
      classOf[immutable.Map[_, _]],
      new ScalaImmutableAbstractMapSerializer(),
      420
    )
  }
}
