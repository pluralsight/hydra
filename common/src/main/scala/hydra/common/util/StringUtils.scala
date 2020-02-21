/*
 * Copyright (C) 2016 Pluralsight, LLC.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package hydra.common.util

/**
  * Created by alexsilva on 11/4/15.
  */
object StringUtils {

  def underscores2camel(name: String): String = {
    require(
      !(name endsWith "_"),
      "names ending in _ not supported by this algorithm."
    )
    "[A-Za-z\\d]+_?|_".r.replaceAllIn(name, { x =>
      val x0 = x.group(0)
      if (x0 == "_") x0
      else x0.stripSuffix("_").toLowerCase.capitalize
    })
  }

  def camel2underscores(x: String): String = {
    "_?[A-Z][a-z\\d]+".r
      .findAllMatchIn(x)
      .map(_.group(0).toLowerCase)
      .mkString("_")
  }
}
