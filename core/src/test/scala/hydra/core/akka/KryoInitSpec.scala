package hydra.core.akka

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}

import com.esotericsoftware.kryo.Kryo
import com.esotericsoftware.kryo.io.{Input, Output}
import com.romix.scala.serialization.kryo.{
  EnumerationSerializer,
  ScalaImmutableAbstractMapSerializer,
  ScalaMutableMapSerializer
}
import org.scalatest.{FlatSpecLike, Matchers}

class KryoInitSpec extends Matchers with FlatSpecLike {

  "The custom KryoInit" should "register serializers" in {
    val kryo = new Kryo()
    new KryoInit().customize(kryo)
    kryo.getDefaultSerializer(classOf[scala.Enumeration#Value]) shouldBe an[
      EnumerationSerializer
    ]
    kryo.getDefaultSerializer(classOf[collection.immutable.Map[_, _]]) shouldBe a[
      ScalaImmutableAbstractMapSerializer
    ]
    kryo.getDefaultSerializer(classOf[collection.immutable.Map[_, _]]) shouldBe a[
      ScalaImmutableAbstractMapSerializer
    ]
    kryo.getDefaultSerializer(classOf[collection.mutable.HashMap[_, _]]) shouldBe a[
      ScalaMutableMapSerializer
    ]
  }

  it should "serialize immutable maps" in {
    val kryo = new Kryo()
    new KryoInit().customize(kryo)
    val map1 = Map(
      "Rome" -> "Italy",
      "London" -> "England",
      "Paris" -> "France",
      "New York" -> "USA",
      "Tokyo" -> "Japan",
      "Peking" -> "China",
      "Brussels" -> "Belgium"
    )
    val map2 = map1 + ("Moscow" -> "Russia")
    val map3 = map2 + ("Berlin" -> "Germany")
    val map4 = map3 + ("Germany" -> "Berlin", "Russia" -> "Moscow")
    roundTrip(map1, kryo)
    roundTrip(map2, kryo)
    roundTrip(map3, kryo)
    roundTrip(map4, kryo)
  }

  def roundTrip[T](obj: T, kryo: Kryo): T = {
    val outStream = new ByteArrayOutputStream()
    val output = new Output(outStream, 4096)
    kryo.writeClassAndObject(output, obj)
    output.flush()

    val input = new Input(new ByteArrayInputStream(outStream.toByteArray), 4096)
    val obj1 = kryo.readClassAndObject(input)

    assert(obj == obj1)

    obj1.asInstanceOf[T]
  }

}
