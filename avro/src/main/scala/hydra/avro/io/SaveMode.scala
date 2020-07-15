package hydra.avro.io

/**
  * /**
  * SaveMode is used to specify the expected behavior of creating a record sink to a data source.
  *
  *
  */
  * Created by alexsilva on 7/11/17.
  */
// $COVERAGE-OFF$
object SaveMode extends Enumeration {
  type SaveMode = Value

  val

  /**
    * Append mode means that when saving a DataFrame to a data source, if data/table already exists,
    * contents of the DataFrame are expected to be appended to existing data.
    *
    * @since 1.3.0
    */
  Append, /**
    * Overwrite mode means that when saving a DataFrame to a data source,
    * if data/table already exists, existing data is expected to be overwritten by the contents of
    * the DataFrame.
    *
    * @since 1.3.0
    */
  Overwrite, /**
    * ErrorIfExists mode means that when saving a DataFrame to a data source, if data already exists,
    * an exception is expected to be thrown.
    *
    * @since 1.3.0
    */
  ErrorIfExists, /**
    * Ignore mode means that when saving a DataFrame to a data source, if data already exists,
    * the save operation is expected to not save the contents of the DataFrame and to not
    * change the existing data.
    *
    * @since 1.3.0
    */
  Ignore = Value
}
// $COVERAGE-ON$
