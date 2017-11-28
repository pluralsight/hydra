package hydra.ingest.test

import hydra.core.transport.HydraRecord

case class TestRecord(destination: String,
                      key: Option[String],
                      payload: String) extends HydraRecord[String, String]