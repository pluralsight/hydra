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

import com.google.common.collect.Lists;
import org.apache.avro.Schema;

import java.util.List;

public class JsonToAvroConversionException extends RuntimeException {

    protected final Object value;

    protected final String fieldName;

    protected transient final Schema schema;

    public JsonToAvroConversionException(String msg, Object value, String fieldName, Schema schema) {
        super(msg);
        this.value = value;
        this.fieldName = fieldName;
        this.schema = schema;
    }


    public JsonToAvroConversionException(Object value, String fieldName, Schema schema) {
        this(String.format("Avro conversion error for field %s, %s for %s", fieldName, value, schema), value,
                fieldName, schema);
    }

    public Schema getSchema() {
        return schema;
    }


    public Object getValue() {
        return value;
    }

    public String getFieldName() {
        return fieldName;
    }


    protected static List<String> getFieldNames(Schema schema) {
        List<String> fields = Lists.newArrayList();

        schema.getFields().forEach(f -> {
            fields.add(f.name());
        });

        return fields;
    }

}