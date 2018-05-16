package hydra.core.monitor

import com.typesafe.config.Config
import kamon.{Kamon, MetricReporter}
import kamon.metric.PeriodSnapshot
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, FlatSpecLike, Matchers}


class HydraMonitorSpec extends Matchers
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

  "The HydraMonitor" should "increment success counters" in {
    HydraMonitor.countSuccess("test")
    eventually {
      reporter.snapshot.metrics.counters.filter(_.name == "test").head.value shouldBe 1
    }
  }

  it should "increment failure counters" in {
    HydraMonitor.countFail("test")
    eventually {
      reporter.snapshot.metrics.counters.filter(_.name == "test_fail").head.value shouldBe 1
    }
  }
}
