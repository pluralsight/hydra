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
import com.google.common.collect.Lists;
import org.apache.avro.Schema;

import java.util.List;

public class InvalidDataTypeException extends JsonToAvroConversionException {

    public InvalidDataTypeException(String fieldName, Object value, Schema schema) {
        super(String.format("Value %s for element '%s' is not of type %s.", value, fieldName, typeInfo(schema)), null,
                fieldName, schema);
    }

    private static String typeInfo(Schema schema) {
        String info = schema.getType().toString();
        List<String> typeNames = Lists.newArrayList();
        if (schema.getType() == Schema.Type.UNION) {
            schema.getTypes().forEach((t) -> {
                typeNames.add(t.getType().getName());
            });

            info = schema.getType().getName() + " [" + Joiner.on(",").join(typeNames) + "]";
        }


        return info;

    }
}