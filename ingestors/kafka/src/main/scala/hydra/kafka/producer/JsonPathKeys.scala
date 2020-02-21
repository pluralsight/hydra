package hydra.kafka.producer

import spray.json.DefaultJsonProtocol._
import spray.json.JsValue
import spray.json.lenses.JsonLenses
import spray.json.lenses.JsonLenses._

/**
  * Created by alexsilva on 2/23/17.
  */
object JsonPathKeys {

  def getKey(key: String, json: String): String = {
    if (key.startsWith("{$.")) {
      val theKey = key.substring(1, key.length - 1)
      val path = JsonLenses.fromPath(theKey)
      json.extract[String](path).mkString
    } else {
      key
    }
  }

  def getKey(key: String, json: JsValue): String = {
    if (key.startsWith("{$.")) {
      val theKey = key.substring(1, key.length - 1)
      val path = JsonLenses.fromPath(theKey)
      json.extract[String](path).mkString
    } else {
      key
    }
  }
}
