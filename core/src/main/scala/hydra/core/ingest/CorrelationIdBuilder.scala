package hydra.core.ingest

import hydra.common.util.Base62

import scala.util.Random

object CorrelationIdBuilder {

  private val b62 = new Base62()

  def generate(n: Long = Math.abs(Random.nextLong)) = {
    b62.encode(n)
  }
}
