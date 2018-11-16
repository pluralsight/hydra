package hydra.common.auth

import java.util.UUID

import org.joda.time.DateTime

object TokenGenerator {
  def generateTokenInfo: TokenInfo = {
    TokenInfo(UUID.randomUUID().toString,
      DateTime.parse("1999-12-31T12:58:35Z"),
      Seq("GroupA", "GroupB"))
  }
}
