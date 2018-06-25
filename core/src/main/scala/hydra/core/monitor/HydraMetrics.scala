package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, RangeSampler}

import scala.collection.concurrent.TrieMap

object HydraMetrics {

  private val counters = new TrieMap[String, Counter]()
  private val rangeSamplers = new TrieMap[String, RangeSampler]()

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

  def rangeSamplerIncrement(metricName: String, transportType: String): Unit = {
    val lookupKey = Seq(metricName, transportType).mkString("-")
    rangeSamplers
      .getOrElseUpdate(lookupKey,
        Kamon.rangeSampler(metricName).refine("id" -> transportType))
      .increment()
  }

  def rangeSamplerDecrement(metricName: String, transportType: String): Unit = {
    val lookupKey = Seq(metricName, transportType).mkString("-")
    rangeSamplers
      .getOrElseUpdate(lookupKey,
        Kamon.rangeSampler(metricName).refine("id" -> transportType))
      .decrement()
  }
}