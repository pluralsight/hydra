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

package com.pluralsight.hydra.avro;

import org.apache.avro.Schema;

public class RequiredFieldMissingException extends JsonToAvroConversionException {

    public RequiredFieldMissingException(String fieldName, Schema schema) {
        super(String.format("Field %s (Type %s) is required, but it was not provided.",
                fieldName,schema.getField(fieldName).schema().getType()), null, fieldName, schema);
    }
}