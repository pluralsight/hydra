package hydra.core.listeners

import com.github.vonnagy.service.container.listener.ContainerLifecycleListener
import com.github.vonnagy.service.container.service.ContainerService
import info.faljse.SDNotify.SDNotify

// $COVERAGE-OFF$
class SystemDNotifyListener extends ContainerLifecycleListener{
  override def onShutdown(container: ContainerService): Unit = {}

  override def onStartup(container: ContainerService): Unit = {
    sys.env.get("NOTIFY_SOCKET").foreach { _ =>

      /**
        * We are migrating from SystemD Start Type of Simple to Notify.
        * Notify requires that our application send SystemD a message when its online.
        */
      SDNotify.sendNotify()
    }
  }
}// $COVERAGE-ON$
