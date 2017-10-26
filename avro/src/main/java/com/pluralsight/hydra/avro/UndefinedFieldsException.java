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

import com.google.common.base.Joiner;
import org.apache.avro.Schema;

import java.util.Set;

/**
 * Used to signal the presence of a field in the json payload that is not defined in the schema.
 */
public class UndefinedFieldsException extends JsonToAvroConversionException {

    public UndefinedFieldsException(Set<String> fieldNames, Schema schema) {
        super(String.format("Field(s) '%s' are not defined in the schema and validation is set to strict. Declared " +
                        "fields are: %s.",
                Joiner.on(",").join(fieldNames),
                Joiner.on(",").join(getFieldNames(schema))), null,
                Joiner.on(",").join(fieldNames), schema);
    }
}