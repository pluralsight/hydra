package hydra.core.monitor

import com.typesafe.config.Config
import kamon.{Kamon, MetricReporter}
import kamon.metric.PeriodSnapshot
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}


class HydraMetricsSpec extends Matchers
  with FlatSpecLike
  with Eventually
  with BeforeAndAfterAll {

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))

  val reporter = new MetricReporter {

    var snapshot: PeriodSnapshot = _

    override def reportPeriodSnapshot(snapshot: PeriodSnapshot): Unit = {
      this.snapshot = snapshot
    }

    override def start(): Unit = {}

    override def stop(): Unit = {}

    override def reconfigure(config: Config): Unit = {}
  }

  override def beforeAll = Kamon.addReporter(reporter)

  override def afterAll = Kamon.stopAllReporters()

  "HydraMetrics" should "increment success counters" in {
    HydraMetrics.countSuccess("hydra-success-count", "test.topic")
    HydraMetrics.countSuccess("hydra-success-count", "test.topic")
    eventually {
      reporter.snapshot.metrics.counters.filter(_.name == "hydra-success-count").head.value shouldBe 2
    }
  }

  it should "increment failure counters" in {
    HydraMetrics.countFail("hydra-fail-count", "test.topic")
    HydraMetrics.countFail("hydra-fail-count", "test.topic")
    eventually {
      reporter.snapshot.metrics.counters.filter(_.name == "hydra-fail-count").head.value shouldBe 2
    }
  }

  it should "increment a range sampler" in {
    HydraMetrics.rangeSamplerIncrement("range-sampler-increment", "test-transport")
    eventually {
      reporter.snapshot.metrics.rangeSamplers.filter(_.name == "range-sampler-increment").head.distribution.count shouldBe 1
    }
  }

  it should "decrement a range sampler" in {
    HydraMetrics.rangeSamplerDecrement("range-sampler-decrement", "test-transport")
    eventually {
      reporter.snapshot.metrics.rangeSamplers.filter(_.name == "range-sampler-decrement").head.distribution.count shouldBe 1
    }
  }
}
