package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}

import scala.collection.mutable

trait HydraMetrics {
  private[core] lazy val counters = new mutable.HashMap[String, Counter]()
  private[core] lazy val gauges = new mutable.HashMap[String, Gauge]()
  private[core] lazy val histograms = new mutable.HashMap[String, Histogram]()

  def getOrCreateCounter(lookupKey: String, metricName: String, tags: => Seq[(String, String)]): Counter = {
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine(tags: _*))
  }

  def getOrCreateGauge(lookupKey: String, metricName: String, tags: => Seq[(String, String)]): Gauge = {
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine(tags: _*))
  }

  def getOrCreateHistogram(lookupKey: String, metricName: String, tags: => Seq[(String, String)]): Histogram = {
    histograms
      .getOrElseUpdate(lookupKey,
        Kamon.histogram(metricName).refine(tags: _*))
  }
}
