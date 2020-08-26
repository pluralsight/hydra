package hydra.core.monitor

import kamon.Kamon
import kamon.metric.{Counter, Gauge, Histogram}
import kamon.tag.TagSet
import scalacache.guava.GuavaCache

import scala.concurrent.{ExecutionContext, Future}

object HydraMetrics {

  import scalacache.modes.scalaFuture._

  type Tags = Seq[(String, String)]

  private[core] lazy val countersCache = GuavaCache[Counter]
  private[core] lazy val gaugesCache = GuavaCache[Gauge]
  private[core] lazy val histogramsCache = GuavaCache[Histogram]

  def getOrCreateCounter(lookupKey: String, metricName: String, tags: => Tags)(
      implicit ec: ExecutionContext
  ): Future[Counter] = {
    countersCache.caching(lookupKey)(ttl = None) {
      Kamon.counter(metricName).withTags(TagSet.from(tags.toMap))
    }
  }

  def getOrCreateGauge(lookupKey: String, metricName: String, tags: => Tags)(
      implicit ec: ExecutionContext
  ): Future[Gauge] = {
    gaugesCache.caching(lookupKey)(ttl = None) {
      Kamon.gauge(metricName).withTags(TagSet.from(tags.toMap))
    }
  }

  def getOrCreateHistogram(
      lookupKey: String,
      metricName: String,
      tags: => Tags
  )(implicit ec: ExecutionContext): Future[Histogram] = {
    histogramsCache.caching(lookupKey)(ttl = None) {
      Kamon.histogram(metricName).withTags(TagSet.from(tags.toMap))
    }
  }

  def incrementCounter(lookupKey: String, metricName: String, tags: => Tags)(
      implicit ec: ExecutionContext
  ): Future[Unit] = {
    getOrCreateCounter(lookupKey, metricName, tags).map(_.increment())
  }

  def incrementGauge(lookupKey: String, metricName: String, tags: => Tags)(
      implicit ec: ExecutionContext
  ): Future[Unit] = {
    getOrCreateGauge(lookupKey, metricName, tags).map(_.increment())
  }

  def decrementGauge(lookupKey: String, metricName: String, tags: => Tags)(
      implicit ec: ExecutionContext
  ): Future[Unit] = {
    getOrCreateGauge(lookupKey, metricName, tags).map(_.decrement())
  }

  def recordToHistogram(
      lookupKey: String,
      metricName: String,
      value: Long,
      tags: => Tags
  )(implicit ec: ExecutionContext): Future[Unit] = {
    getOrCreateHistogram(lookupKey, metricName, tags).map(_.record(value))
  }

  def addPromHttpMetric(topic: String, responseCode: String, path: String)(implicit ec: ExecutionContext): Unit = {
    incrementGauge(
      lookupKey =
        s"_${topic}_${responseCode}_${path}",
        metricName = "ingest_topic_response",
        tags = Seq(
          "topic" -> topic,
          "responseCode" -> responseCode,
          "path" -> path
        )
    )
  }
}
