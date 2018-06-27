package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}

import scala.collection.concurrent.TrieMap

object HydraMetrics {

  private[core] val counters = new TrieMap[String, Counter]()
  private[core] val gauges = new TrieMap[String, Gauge]()
  private[core] val histograms = new TrieMap[String, Histogram]()

  def incrementCounter(metricName: String, tags: (String, String)*): Unit = {
    val lookupKey = (Seq(metricName) ++ tags).mkString("-")
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine(tags: _*))
      .increment()
  }

  def incrementGauge(metricName: String, tags: (String, String)*): Unit = {
    val lookupKey = (Seq(metricName) ++ tags).mkString("-")
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
      .increment()
  }

  def decrementGauge(metricName: String, tags: (String, String)*): Unit = {
    val lookupKey = (Seq(metricName) ++ tags).mkString("-")
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
      .decrement()
  }

  def histogramRecord(metricName: String, value: Long, tags: (String, String)*): Unit = {
    histograms
      .getOrElseUpdate(metricName,
        Kamon.histogram(metricName).refine(tags: _*))
      .record(value)
  }

  def getOrCreateCounter(metricName: String, tags: (String, String)*): Counter = {
    val lookupKey = (Seq(metricName) ++ tags).mkString("-")
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine(tags: _*))
  }

  def getOrCreateGauge(metricName: String, tags: (String, String)*): Gauge = {
    val lookupKey = (Seq(metricName) ++ tags).mkString("-")
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
  }

}
