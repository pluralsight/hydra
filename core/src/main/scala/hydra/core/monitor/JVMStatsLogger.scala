package hydra.core.monitor

import hydra.common.logging.LoggingAdapter

import java.lang.management.ManagementFactory
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success, Try}

object JVMStatsLogger extends LoggingAdapter {

  private val countBytesInMB = Math.pow(1024, 2)

  def logJVMStats(): Unit = doLogJVMStats() match {
    case Success(_) => ()
    case Failure(e) => log.warn("Failed to get and log JVM Stats", e)
  }

  private def doLogJVMStats(): Try[Unit] = Try {
    //due to implementation is static variable and will be created if null
    val memoryMXBean = ManagementFactory.getMemoryMXBean
    val operatingSystemMXBean = ManagementFactory.getOperatingSystemMXBean

    log.info(s"The memory heap usage [${memoryMXBean.getHeapMemoryUsage.getUsed.toMB} MB] of maximum heap memory [${memoryMXBean.getHeapMemoryUsage.getMax.toMB}] MB")
    log.info(s"The memory non-heap usage [${memoryMXBean.getNonHeapMemoryUsage.getUsed.toMB} MB] of maximum non-heap memory [${memoryMXBean.getNonHeapMemoryUsage.getMax.toMB}] MB")
    log.info(s"The system CPU average load [${operatingSystemMXBean.getSystemLoadAverage} %].")
  }

  implicit class BytesToMB(value: Long) {
    def toMB: Double =
      if (value <= 0) {
        0
      } else {
        value / countBytesInMB
      }
  }

  implicit class LogJVMStatsInFuture[A](value: Future[A]) {
    /**
     * logs JVM stats and returns given future's result
     * @param ec - implicit ExecutionContext
     * @return value of the given future
     */
    def logJVMStats(implicit ec: ExecutionContext): Future[A] = value.transform(res => {
      JVMStatsLogger.logJVMStats(); res
    })
  }

}
