package hydra.core.akka

import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo.EnumerationSerializer
import com.romix.scala.serialization.kryo.ScalaImmutableMapSerializer

class KryoInit {
  def customize(kryo: Kryo): Unit = {
    kryo.addDefaultSerializer(classOf[scala.Enumeration#Value], classOf[EnumerationSerializer])
    kryo.addDefaultSerializer(classOf[scala.collection.Map[_, _]], classOf[ScalaImmutableMapSerializer])
  }
}