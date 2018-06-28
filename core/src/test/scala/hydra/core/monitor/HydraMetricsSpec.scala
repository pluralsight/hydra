package hydra.core.monitor

import com.typesafe.config.Config
import kamon.metric.{Counter, Gauge, Histogram, PeriodSnapshot}
import kamon.{Kamon, MetricReporter}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.mutable
import scala.reflect.ClassTag


class HydraMetricsSpec extends Matchers
  with FlatSpecLike
  with Eventually
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockFactory
  with HydraMetrics {


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
    gauges.clear()
    counters.clear()
    histograms.clear()
  }

  override def afterAll = Kamon.stopAllReporters()

  "An object mixing in HydraMetrics" should "get or create a counter" in {
    harness[Counter]("test.counters", counters)(getOrCreateCounter _)
  }

  it should "get or create a gauge" in {
    harness[Gauge]("test.gauges", gauges)(getOrCreateGauge _)
  }

  // Can't use the harness for this test since Kamon always returns the same histogram instance.
  it should "get or create a histogram" in {
    val metricName = "test.histogram"

    val h = getOrCreateHistogram("test.1.lookup", metricName, Seq("tag1" -> "success"))
    h shouldBe a[Histogram]
    histograms.size shouldBe 1

    val h2 = getOrCreateHistogram("test.1.lookup", metricName, Seq("tag1" -> "fail"))
    h2 shouldEqual h
    histograms.size shouldBe 1
  }

  def harness[A: ClassTag](metricName: String, internalMap: mutable.HashMap[String, A])
                          (f: (String, String, => Seq[(String, String)]) => A): Assertion = {
    val metric = f("test.1.lookup", metricName, Seq("tag1" -> "success"))
    metric shouldBe a[A]
    internalMap.size shouldBe 1

    val metric2 = f("test.1.lookup", metricName, Seq("tag1" -> "success"))
    metric2 shouldEqual metric
    internalMap.size shouldBe 1

    val metric3 = f("test.2.lookup", metricName, Seq("tag1" -> "fail", "tag2" -> "other"))
    metric3 should not equal metric
    internalMap.size shouldBe 2
  }
}
