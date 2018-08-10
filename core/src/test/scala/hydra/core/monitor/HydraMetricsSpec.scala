package hydra.core.monitor

import akka.japi.Option.Some
import kamon.Kamon
import kamon.metric.{Counter, Gauge}
import org.scalamock.scalatest.proxy.MockFactory
import org.scalatest._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Millis, Seconds, Span}
import scalacache.guava.GuavaCache

import scala.util.Random


class HydraMetricsSpec extends Matchers
  with FlatSpecLike
  with Eventually
  with BeforeAndAfterAll
  with BeforeAndAfterEach
  with MockFactory {

  import HydraMetrics._
  import scalacache.modes.try_._

  implicit override val patienceConfig =
    PatienceConfig(timeout = scaled(Span(2, Seconds)), interval = scaled(Span(5, Millis)))

  override def beforeEach() = {
    gaugesCache.removeAll()
    countersCache.removeAll()
    histogramsCache.removeAll()
  }

  override def afterAll = Kamon.stopAllReporters()

  val lookup = "lookup.xyz"
  val lookup2 = "lookup.abc"

  def generateTags: Seq[(String, String)] = Seq("tag1" -> "Everything's fine.")

  "An object mixing in HydraMetrics" should
    "create new counters with new lookup keys + metric names" in {
    shouldCreateNewMetric[Counter](incrementCounter _, countersCache)
  }

  it should
    "create new gauges with new lookup keys + metric names" in {
    shouldCreateNewMetric[Gauge](incrementGauge _, gaugesCache)
  }

  it should "lookup existing counters" in {
    shouldLookupExistingMetric[Counter](incrementCounter _, countersCache)
  }

  it should
    "lookup an existing gauge" in {
    shouldLookupExistingMetric[Gauge](decrementGauge _, gaugesCache)
  }

  it should
    "create a new histogram" in {
    histogramsCache.get(lookup).map { result =>
      result shouldBe None
    }

    recordToHistogram(lookup, "histogram.metric", 100, generateTags)

    histogramsCache.get(lookup).map { result =>
      result shouldBe a[Some[_]]
    }
  }

  it should
    "lookup an existing histogram" in {
    val f = recordToHistogram _

    f(lookup, "histogram.metric", 100, generateTags) shouldEqual
      f(lookup, "histogram.metric", 100, generateTags)
  }

  private def shouldCreateNewMetric[A](f: (String, String, => Seq[(String, String)]) => Unit, cache: GuavaCache[A]) = {
    cache.get(lookup).map { result =>
      result shouldBe None
    }

    f(lookup, "metric" + Random.nextInt(Integer.MAX_VALUE), generateTags)

    cache.get(lookup).map { result =>
      result shouldBe a[Some[_]]
    }
  }

  private def shouldLookupExistingMetric[A](f: (String, String, => Seq[(String, String)]) => Unit, cache: GuavaCache[A]) = {
    val metric = "metric" + Random.nextInt(Integer.MAX_VALUE)

    f(lookup, metric, generateTags) shouldEqual f(lookup, metric, generateTags)
  }
}
