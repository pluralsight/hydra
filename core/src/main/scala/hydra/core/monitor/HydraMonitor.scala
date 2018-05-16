package hydra.core.monitor

import kamon.Kamon
import kamon.metric.CounterMetric

import scala.collection.concurrent.TrieMap

object HydraMonitor {

  private val counters = new TrieMap[String, CounterMetric]()

  def countSuccess(metricName: String): Unit = {
    counters.getOrElseUpdate(metricName, Kamon.counter(metricName)).increment()
  }

  def countFail(metricName: String): Unit = {
    val name = metricName + "_fail"
    counters.getOrElseUpdate(name, Kamon.counter(name)).increment()
  }

}
