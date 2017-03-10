package hydra.ingest.ws

/**
  * Created by alexsilva on 3/10/17.
  */

import scala.language.implicitConversions

package object chat {
  implicit def chatEventToChatMessage(event: IncomingMessage): ChatMessage = ChatMessage(event.sender, event.message)
}