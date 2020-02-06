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

import com.google.common.collect.ImmutableList;
import org.apache.avro.JsonProperties;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.avro.generic.GenericRecord;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

public class JsonConverterTest {
    private static Schema.Field sf(String name, Schema schema) {
        return new Schema.Field(name, schema, "");
    }

    private static Schema.Field sfnull(String name, Schema schema) {
        return new Schema.Field(name, schema, "", JsonProperties.NULL_VALUE);
    }

    private static Schema.Field sf(String name, Type type) {
        return sf(name, sc(type));
    }

    private static Schema sc(Type type) {
        return Schema.create(type);
    }

    private static Schema sr(Schema.Field... fields) {
        return Schema.createRecord("testName", "", "testNamespace", false, Arrays.asList(fields));
    }


    Schema.Field f1 = sf("field1", Type.LONG);

    Schema.Field f2 = sf("field2", Schema.createArray(sc(Type.BOOLEAN)));

    Schema.Field f3Map = sf("field3", Schema.createMap(sc(Type.STRING)));

    Schema.Field f3Rec = sf("field3", sr(sf("key", Type.STRING)));

    Schema.Field fn = sf("fieldN", Type.NULL);


    @Test
    public void testNullFields() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, fn));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"fieldN\": null}";
        GenericRecord r = jc.convert(json);
        assertEquals(json, r.toString());
        assertEquals(0, jc.getConversionStats().getMissingFields().size());
    }

    /**
     * This test addresses the case when a field is set to be of NULL type in a schema but a non-null
     * value is provided.
     * @throws Exception
     */
    @Test(expected = InvalidDataTypeException.class)
    public void testNullMismatch() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, fn));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"fieldN\": \"123\"}";
        GenericRecord r = jc.convert(json);
        assertEquals(json, r.toString());
        assertEquals(0, jc.getConversionStats().getMissingFields().size());
    }


    @Test
    public void testBasicWithMap() throws Exception {

        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Map));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
        GenericRecord r = jc.convert(json);
        assertEquals(json, r.toString());
        assertEquals(0, jc.getConversionStats().getMissingFields().size());
    }

    @Test
    public void testBasicWithRecord() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
        GenericRecord r = jc.convert(json);
        assertEquals(json, r.toString());
        assertEquals(0, jc.getConversionStats().getMissingFields().size());
    }

    @Test(expected = RequiredFieldMissingException.class)
    public void testMissingRequiredField() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec));
        String json = "{\"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
        jc.convert(json);
    }

    @Test(expected = InvalidDataTypeException.class)
    public void testWrongDataTypeField() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec));
        String json = "{ \"field1\": \"test\", \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
        jc.convert(json);
    }

    @Test
    public void testMissingNullableField() throws Exception {
        Schema optional = Schema.createUnion(ImmutableList.of(Schema.create(Type.NULL), Schema.create(Type.DOUBLE)));
        Schema.Field f4 = sf("field4", optional);
        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec, f4));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}}";
        GenericRecord r = jc.convert(json);
        String expect = "{\"field1\": 1729, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}, " +
                "\"field4\": null}";
        assertEquals(expect, r.toString());
        assertEquals(1, jc.getConversionStats().getMissingFields().size());
        assertEquals(0, jc.getConversionStats().getMissingFieldCount("field1"));
        assertEquals(1, jc.getConversionStats().getMissingFieldCount("field4"));
    }

    @Test
    public void testTreatNullAsMissing() throws Exception {
        Schema optional = Schema.createUnion(ImmutableList.of(Schema.create(Type.NULL), Schema.create(Type.DOUBLE)));
        Schema.Field f4 = sf("field4", optional);
        JsonConverter jc = new JsonConverter(sr(f1, f2, f4));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false], \"field4\": null}";
        GenericRecord r = jc.convert(json);
        String expect = "{\"field1\": 1729, \"field2\": [true, true, false], \"field4\": null}";
        assertEquals(expect, r.toString());
        assertEquals(1, jc.getConversionStats().getMissingFields().size());
        assertEquals(0, jc.getConversionStats().getMissingFieldCount("field1"));
        assertEquals(1, jc.getConversionStats().getMissingFieldCount("field4"));
    }

    @Test
    public void testNullDefaultValue() throws Exception {
        Schema optional = Schema.createUnion(ImmutableList.of(Schema.create(Type.NULL), Schema.create(Type.STRING)));
        Schema.Field f4 = sfnull("field4", optional);
        JsonConverter jc = new JsonConverter(sr(f1, f2, f4));
        String json = "{\"field1\": 1729, \"field2\": [true, true, false]}";
        GenericRecord r = jc.convert(json);
        String expect = "{\"field1\": 1729, \"field2\": [true, true, false], \"field4\": null}";
        assertEquals(expect, r.toString());
        assertEquals(1, jc.getConversionStats().getMissingFieldsCount());
        assertEquals(0, jc.getConversionStats().getMissingFieldCount("field1"));
        assertEquals(1, jc.getConversionStats().getMissingFieldCount("field4"));
    }

    @Test
    public void testNotInSchemaValues() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec));
        String json = "{ \"field1\": 1, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}," + " " +
                "\"extra\": \"test\"}";
        jc.convert(json);
        assertEquals(0, jc.getConversionStats().getMissingFieldsCount());
        assertEquals(1, jc.getConversionStats().getUnknownFieldsCount());
        assertEquals(1, jc.getConversionStats().getUnknownFieldCount("extra"));
    }

    @Test(expected = UndefinedFieldsException.class)
    public void testStrictValidation() throws Exception {
        JsonConverter jc = new JsonConverter(sr(f1, f2, f3Rec), true);
        String json = "{ \"field1\": 1, \"field2\": [true, true, false], \"field3\": {\"key\": \"value\"}," + " " +
                "\"extra\": \"test\"}";
        jc.convert(json);
    }
}