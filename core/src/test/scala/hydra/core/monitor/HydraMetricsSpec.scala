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

  "HydraMetrics" should "increment success and fail counters with the same metricName" in {
    HydraMetrics.countSuccess("hydra-count", "test.topic")
    HydraMetrics.countFail("hydra-count", "test.topic")
    eventually {
      reporter.snapshot.metrics.counters.filter(_.tags("type") == "success").head.value shouldBe 1
      reporter.snapshot.metrics.counters.filter(_.tags("type") == "fail").head.value shouldBe 1
    }
  }

  it should "increment/decrement a gauge with the same metricName" in {
    val metricName = "hydra_ingest_journal_message_count"
    val transportType = "KafkaTransport"
    HydraMetrics.incrementGauge(metricName, transportType)
    HydraMetrics.incrementGauge(metricName, transportType)
    HydraMetrics.decrementGauge(metricName, transportType)
    eventually {
      reporter.snapshot.metrics.gauges.filter(_.tags("id") == transportType).head.value shouldBe 1
    }
  }

  it should "record histogram metrics" in {
    val metricName = "hydra_ingest_histogram_test"
    HydraMetrics.histogramRecord(metricName)
    HydraMetrics.histogramRecord(metricName)
    eventually {
      reporter.snapshot.metrics.histograms.filter(_.name == metricName).head.distribution.count shouldBe 2
    }
  }

}
