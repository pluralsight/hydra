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

import java.io.Closeable

/**
  * Created by alexsilva on 1/10/17.
  */
object Resource {

  def using[A <: Closeable, B](resource: A)(f: A => B): B = {
    require(resource != null, "The supplied resource was null.")
    try {
      f(resource)
    } finally {
      resource.close()
    }
  }
}
