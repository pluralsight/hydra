package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}

import scala.collection.mutable

object HydraMetrics {
  type Tags = Seq[(String, String)]

  private[core] lazy val counters = new mutable.HashMap[String, Counter]()
  private[core] lazy val gauges = new mutable.HashMap[String, Gauge]()
  private[core] lazy val histograms = new mutable.HashMap[String, Histogram]()

  def getOrCreateCounter(lookupKey: String, metricName: String, tags: => Tags): Counter = {
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine(tags: _*))
  }

  def getOrCreateGauge(lookupKey: String, metricName: String, tags: => Tags): Gauge = {
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
  }

  def getOrCreateHistogram(lookupKey: String, metricName: String, tags: => Tags): Histogram = {
    histograms
      .getOrElseUpdate(lookupKey,
        Kamon.histogram(metricName).refine(tags: _*))
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
