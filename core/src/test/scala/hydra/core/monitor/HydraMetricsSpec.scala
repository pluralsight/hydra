package hydra.core.monitor

import com.typesafe.config.Config
import kamon.metric.{Counter, Gauge, PeriodSnapshot}
import kamon.{Kamon, MetricReporter}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}

import scala.collection.mutable
import scala.util.Random


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

  val lookup = "lookup.xyz"
  val lookup2 = "lookup.abc"

  def generateTags: Seq[(String, String)] = Seq("tag1" -> "Everything's fine.")

  "An object mixing in HydraMetrics" should
    "create new counters with new lookup keys + metric names" in {
    shouldCreateNewMetric[Counter](incrementCounter _, counters)
  }

  it should
    "create new gauges with new lookup keys + metric names" in {
    shouldCreateNewMetric[Gauge](incrementGauge _, gauges)
  }

  it should "lookup existing counters" in {
    shouldLookupExistingMetric[Counter](incrementCounter _, counters)
  }

  it should
    "lookup an existing gauge" in {
    shouldLookupExistingMetric[Gauge](decrementGauge _, gauges)
  }

  it should
    "create a new histogram" in {
    recordToHistogram(lookup, "histogram.metric", 100, generateTags)

    histograms.size shouldBe 1

    recordToHistogram(lookup2, "histogram.metric", 250, Seq("tag2" -> "Let it burn!"))

    histograms.size shouldBe 2
  }

  it should
    "lookup an existing histogram" in {
    for (x <- 1 to 2) {
      recordToHistogram(lookup, "histogram.metric", 100, generateTags)

      histograms.size shouldBe 1
    }
  }

  private def shouldCreateNewMetric[A](f: (String, String, => Seq[(String, String)]) => Unit, map: mutable.HashMap[String, A]) = {
    f(lookup, "metric" + Random.nextInt(Integer.MAX_VALUE), generateTags)
    map.size shouldBe 1

    f(lookup2, "metric" + Random.nextInt(Integer.MAX_VALUE), generateTags)
    map.size shouldBe 2
  }

  private def shouldLookupExistingMetric[A](f: (String, String, => Seq[(String, String)]) => Unit, map: mutable.HashMap[String, A]) = {
    val metric = "metric" + Random.nextInt(Integer.MAX_VALUE)
    for (x <- 1 to 2) {
      f(lookup, metric, generateTags)

      map.size shouldBe 1
    }
  }
}
