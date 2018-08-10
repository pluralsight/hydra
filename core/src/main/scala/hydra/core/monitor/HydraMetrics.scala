package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}
import scalacache.guava.GuavaCache

import scala.util.{Failure, Success}

object HydraMetrics {

  import scalacache.modes.try_._

  type Tags = Seq[(String, String)]

  private[core] lazy val countersCache = GuavaCache[Counter]
  private[core] lazy val gaugesCache = GuavaCache[Gauge]
  private[core] lazy val histogramsCache = GuavaCache[Histogram]

  def getOrCreateCounter(lookupKey: String, metricName: String, tags: => Tags): Counter = {
    countersCache.caching(lookupKey)(ttl = None) {
      Kamon.counter(metricName).refine(tags: _*)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  def getOrCreateGauge(lookupKey: String, metricName: String, tags: => Tags): Gauge = {
    gaugesCache.caching(lookupKey)(ttl = None) {
      Kamon.gauge(metricName).refine(tags: _*)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  def getOrCreateHistogram(lookupKey: String, metricName: String, tags: => Tags): Histogram = {
    histogramsCache.caching(lookupKey)(ttl = None) {
      Kamon.histogram(metricName).refine(tags: _*)
    } match {
      case Success(value) => value
      case Failure(exception) => throw exception
    }
  }

  def incrementCounter(lookupKey: String, metricName: String, tags: => Tags): Unit = {
    getOrCreateCounter(lookupKey, metricName, tags).increment()
  }

  def incrementGauge(lookupKey: String, metricName: String, tags: => Tags): Unit = {
    getOrCreateGauge(lookupKey, metricName, tags).increment()
  }


  def decrementGauge(lookupKey: String, metricName: String, tags: => Tags): Unit = {
    getOrCreateGauge(lookupKey, metricName, tags).decrement()
  }

  def recordToHistogram(lookupKey: String, metricName: String, value: Long, tags: => Tags): Unit = {
    getOrCreateHistogram(lookupKey, metricName, tags).record(value)
  }
}
