/*
 * Copyright (C) 2017 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package hydra.common.util

/**
 * Created by alexsilva on 12/7/15.
 */
object AvroUtils {

  val pattern = "^(?!\\d|[a-zA-Z]|_)".r

  /**
   * Valid fields in Avro need to start with a number, an underscore or a letter.  This function checks the
   * field name and replaces the first character with an underscore if it is not valid.
   * @param name
   */
  def cleanName(name: String) = {
    pattern findFirstIn name match {
      case Some(str) => "_" + name.substring(1)
      case None => name
    }
  }
}
