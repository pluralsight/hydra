package test.scan

import hydra.core.bootstrap.ServiceProvider

class TestServiceProvider extends ServiceProvider {

  /**
    * @return The list of services to be instantiated.
    */
  override def services = Seq.empty
}
