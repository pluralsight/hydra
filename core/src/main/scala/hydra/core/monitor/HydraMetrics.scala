package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}
import kamon.metric.MeasurementUnit._

import scala.collection.concurrent.TrieMap

object HydraMetrics {

  private val counters = new TrieMap[String, Counter]()
  private val gauges = new TrieMap[String, Gauge]()
  private val histograms = new TrieMap[String, Histogram]()

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

  def histogramRecord(metricName: String, tags: (String, String)*): Unit = {
    histograms
      .getOrElseUpdate(metricName,
        Kamon.histogram(metricName).refine(tags: _*))
      .record(1)
  }

}
