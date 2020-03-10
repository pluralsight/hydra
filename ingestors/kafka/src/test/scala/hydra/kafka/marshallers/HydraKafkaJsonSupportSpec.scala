package hydra.kafka.marshallers

import org.apache.kafka.common.{Node, PartitionInfo}
import org.scalatest.matchers.should.Matchers
import org.scalatest.funspec.AnyFunSpecLike

class HydraKafkaJsonSupportSpec
    extends Matchers
    with AnyFunSpecLike
    with HydraKafkaJsonSupport {

  import spray.json._

  describe("When marshalling kafka objects") {
    it("converts Nodes") {
      val node = new Node(1, "host", 9092)
      node.toJson shouldBe """{"id":1,"host":"host","port":9092}""".parseJson
      val nodeJ =
        """{"id":1,"host":"host","port":9092}""".parseJson.convertTo[Node]
      nodeJ.id shouldBe 1
      nodeJ.host shouldBe "host"
      nodeJ.port shouldBe 9092

    }

    it("converts Partitions") {
      val node = new Node(1, "host", 9092)
      val p = new PartitionInfo("topic", 0, node, Array(node), Array(node))
      p.toJson shouldBe """{"partition":0,"leader":{"id":1,"host":"host","port":9092},"isr":[[{"id":1,"host":"host","port":9092}]]}""".parseJson
      intercept[NotImplementedError] {
        """{"partition":0,"leader":{"id":1,"host":"host","port":9092},"isr":[[{"id":1,"host":"host","port":9092}]]}""".parseJson
          .convertTo[PartitionInfo]
      }
    }

  }

}
