package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}

import scala.collection.mutable

trait HydraMetrics {
  private[core] lazy val counters = new mutable.HashMap[String, Counter]()
  private[core] lazy val gauges = new mutable.HashMap[String, Gauge]()
  private[core] lazy val histograms = new mutable.HashMap[String, Histogram]()

  def incrementCounter(lookupKey: String, metricName: String, tags: => Seq[(String, String)]): Unit = {
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine(tags: _*))
      .increment()
  }

  def incrementGauge(lookupKey: String, metricName: String, tags: => Seq[(String, String)]): Unit = {
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
      .increment()
  }

  def decrementGauge(lookupKey: String, metricName: String, tags: => Seq[(String, String)]): Unit = {
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
      .decrement()
  }

  def recordToHistogram(lookupKey: String, metricName: String, value: Long, tags: => Seq[(String, String)]): Unit = {
    histograms
      .getOrElseUpdate(lookupKey,
        Kamon.histogram(metricName).refine(tags: _*))
      .record(value)
  }
}
