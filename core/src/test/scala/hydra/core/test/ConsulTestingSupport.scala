package hydra.core.test

import com.google.common.net.HostAndPort
import com.orbitz.consul.Consul
import com.pszymczyk.consul.ConsulStarterBuilder
import hydra.common.config.ConfigSupport
import hydra.common.logging.LoggingAdapter
import org.scalatest.{BeforeAndAfterAll, TestSuite}

trait ConsulTestingSupport extends TestSuite
  with ConfigSupport
  with LoggingAdapter
  with BeforeAndAfterAll {

  val consulPort = rootConfig.getInt("consul.http.port")
  val consulAddress = rootConfig.getString("consul.http.address")

  lazy val consul = ConsulStarterBuilder.consulStarter()
    .withHttpPort(consulPort).build().start()

  lazy val consulClient = Consul.builder()
    .withHostAndPort(HostAndPort.fromParts(consulAddress, consulPort)).build()

  override def beforeAll() = log.debug(s"Started consul at ${consul.getAddress}:${consul.getHttpPort}.")

  override def afterAll() = {
    consul.close()
  }
}
