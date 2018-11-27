package hydra.common.auth

import java.util.UUID

object TokenGenerator {
  def generateTokenInfo: TokenInfo = {
    TokenInfo(UUID.randomUUID().toString,
      Set("resourceA", "resourceB"))
  }
}
