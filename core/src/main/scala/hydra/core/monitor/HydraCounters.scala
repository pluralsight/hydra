package hydra.core.monitor

import kamon.Kamon
import kamon.metric.Counter

import scala.collection.concurrent.TrieMap

object HydraCounters {

  private val counters = new TrieMap[String, Counter]()

  def countSuccess(metricName: String, destination: String): Unit = {
    val result = "success"
    val lookupKey = metricName + destination + result
    counters.getOrElseUpdate(lookupKey, Kamon.counter(metricName).refine(("type", result), ("destination", destination))).increment()
  }

  def countFail(metricName: String, destination: String): Unit = {
    val result = "fail"
    val lookupKey = metricName + destination + result
    counters.getOrElseUpdate(lookupKey, Kamon.counter(metricName).refine(("type", result), ("destination", destination))).increment()
  }

}
