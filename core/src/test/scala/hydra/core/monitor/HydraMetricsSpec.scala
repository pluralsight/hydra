package hydra.core.monitor

import com.typesafe.config.Config
import kamon.{Kamon, MetricReporter}
import kamon.metric.PeriodSnapshot
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import org.scalatest.{BeforeAndAfterAll, BeforeAndAfterEach, FlatSpecLike, Matchers}


class HydraMetricsSpec extends Matchers
  with FlatSpecLike
  with Eventually
  with BeforeAndAfterAll
  with BeforeAndAfterEach {


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

  override def beforeEach() = {
    HydraMetrics.gauges.clear()
    HydraMetrics.counters.clear()
    HydraMetrics.histograms.clear()
  }

  override def afterAll = Kamon.stopAllReporters()

  "HydraMetrics" should "increment success and fail counters with the same metricName" in {
    HydraMetrics.incrementCounter("hydra-count", "destination" -> "test.topic", "type" -> "success")
    HydraMetrics.incrementCounter("hydra-count", "destination" -> "test.topic", "type" -> "fail")
    eventually {
      reporter.snapshot.metrics.counters.filter(_.tags("type") == "success").head.value shouldBe 1
      reporter.snapshot.metrics.counters.filter(_.tags("type") == "fail").head.value shouldBe 1
    }
  }

  it should "increment/decrement a gauge with the same metricName" in {
    val metricName = "hydra_ingest_journal_message_count"
    val transportType = "KafkaTransport"
    HydraMetrics.incrementGauge(metricName, "id" -> transportType)
    HydraMetrics.incrementGauge(metricName, "id" -> transportType)
    HydraMetrics.decrementGauge(metricName, "id" -> transportType)
    eventually {
      reporter.snapshot.metrics.gauges.filter(_.tags("id") == transportType).head.value shouldBe 1
    }
  }

  it should "record histogram metrics" in {
    val metricName = "hydra_ingest_histogram_test"
    HydraMetrics.histogramRecord(metricName, 100L, "transport" -> "TestTransport")
    HydraMetrics.histogramRecord(metricName, 50L, "transport" -> "TestTransport")
    eventually {
      reporter.snapshot.metrics.histograms.filter(_.name == metricName).head.distribution.sum shouldBe 150L
      reporter.snapshot.metrics.histograms.filter(_.name == metricName).head.distribution.count shouldBe 2
    }
  }

  it should "use the local cache for maintaining counters in" in {
    HydraMetrics.counters.size shouldBe 0
    val _ =  HydraMetrics.getOrCreateCounter("hydra-count", "destination" -> "test.topic", "type" -> "success")
    HydraMetrics.counters.size shouldBe 1
    HydraMetrics.incrementCounter("hydra-count", "destination" -> "test.topic", "type" -> "success")
    HydraMetrics.counters.size shouldBe 1
  }

  it should "use the local cache for maintaining gauges in" in {
    HydraMetrics.gauges.size shouldBe 0
    val _ =  HydraMetrics.getOrCreateGauge("hydra-gauge", "destination" -> "test.topic")
    HydraMetrics.gauges.size shouldBe 1
    HydraMetrics.incrementGauge("hydra-gauge", "destination" -> "test.topic")
    HydraMetrics.gauges.size shouldBe 1
  }

}
