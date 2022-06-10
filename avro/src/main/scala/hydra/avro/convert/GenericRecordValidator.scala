package hydra.avro.convert

import org.apache.avro.generic.{GenericData, GenericDatumReader}
import org.apache.avro.io.ResolvingDecoder
import org.apache.avro.{AvroTypeException, Schema}

class GenericRecordValidator[T](schema: Schema) extends GenericDatumReader[T](schema) {
  override def readField(r: scala.Any,
                         f: Schema.Field,
                         oldDatum: scala.Any,
                         in: ResolvingDecoder,
                         state: scala.Any): Unit = {
    try {
      super.readField(r, f, oldDatum, in, state)
    } catch {
      case t: AvroTypeException =>
        if (f.defaultVal == null) {
          throw new AvroTypeException(s"${f.name()} -> ${t.getMessage}")
        } else {
          throw t
        }
    }
  }
}