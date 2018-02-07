package hydra.core.akka

import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo.{EnumerationSerializer, ScalaImmutableAbstractMapSerializer, ScalaMutableMapSerializer}
import hydra.common.logging.LoggingAdapter

class KryoInit extends LoggingAdapter {
  def customize(kryo: Kryo): Unit = {
    log.debug("Initializing Kryo...")
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.mutable.HashMap[_, _]], classOf[ScalaMutableMapSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.Map[_, _]], classOf[ScalaImmutableAbstractMapSerializer])
  }
}