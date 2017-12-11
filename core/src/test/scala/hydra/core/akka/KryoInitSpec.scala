package hydra.core.akka

import com.esotericsoftware.kryo.Kryo
import com.romix.scala.serialization.kryo.{EnumerationSerializer, ScalaImmutableMapSerializer}
import org.scalatest.{FlatSpecLike, Matchers}

class KryoInitSpec extends Matchers with FlatSpecLike {

  "The custom KryoInit" should "register serializers" in {
    val kryo = new Kryo()
    new KryoInit().customize(kryo)
    kryo.getDefaultSerializer(classOf[scala.Enumeration#Value]) shouldBe an[EnumerationSerializer]
    kryo.getDefaultSerializer(classOf[scala.collection.Map[_, _]]) shouldBe a[ScalaImmutableMapSerializer]
  }

}

