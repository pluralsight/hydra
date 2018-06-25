package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge}

import scala.collection.concurrent.TrieMap

object HydraMetrics {

  private val counters = new TrieMap[String, Counter]()
  private val gauges = new TrieMap[String, Gauge]()

  def countSuccess(metricName: String, destination: String): Unit = {
    val result = "success"
    val lookupKey = Seq(metricName, destination, result).mkString("-")
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine("type" -> result, "destination" -> destination))
      .increment()
  }

  def countFail(metricName: String, destination: String): Unit = {
    val result = "fail"
    val lookupKey = Seq(metricName, destination, result).mkString("-")
    counters
      .getOrElseUpdate(lookupKey,
        Kamon.counter(metricName).refine("type" -> result, "destination" -> destination))
      .increment()
  }

  def incrementGauge(metricName: String, transportType: String): Unit = {
    val lookupKey = Seq(metricName, transportType).mkString("-")
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine("id" -> transportType))
      .increment()
  }

  def decrementGauge(metricName: String, transportType: String): Unit = {
    val lookupKey = Seq(metricName, transportType).mkString("-")
    gauges
      .getOrElseUpdate(lookupKey,
        Kamon.gauge(metricName).refine("id" -> transportType))
      .decrement()
  }

}