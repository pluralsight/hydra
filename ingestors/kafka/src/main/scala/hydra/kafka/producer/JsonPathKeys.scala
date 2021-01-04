package hydra.kafka.producer

import spray.json.{JsValue, JsonParser}


/**
  * Created by alexsilva on 2/23/17.
  */
object JsonPathKeys {

  def getKey(key: String, json: String): String = {
    if (key.startsWith("{$.")) {
      val theKey = key.substring(3, key.length - 1)
      JsonParser(json).asJsObject.fields.getOrElse(theKey, "").toString.replace("\"","")
    } else {
      key
    }
  }

  def getKey(key: String, json: JsValue): String = {
    if (key.startsWith("{$.")) {
      val theKey = key.substring(3, key.length - 1)
      json.asJsObject.fields.getOrElse(theKey, "").toString.replace("\"","")
    } else {
      key
    }
  }
}
