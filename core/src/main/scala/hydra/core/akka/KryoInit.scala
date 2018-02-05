package hydra.core.akka

import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo.{EnumerationSerializer, ScalaImmutableAbstractMapSerializer, ScalaMutableMapSerializer}

class KryoInit {
  def customize(kryo: Kryo): Unit = {
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.mutable.HashMap[_, _]], classOf[ScalaMutableMapSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.Map[_, _]], classOf[ScalaImmutableAbstractMapSerializer])
  }
}