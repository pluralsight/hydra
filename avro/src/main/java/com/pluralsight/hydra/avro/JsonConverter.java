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

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.util.ClassUtil;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import hydra.avro.util.AvroUtils;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.util.internal.JacksonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.avro.Schema.Field;
import static org.apache.avro.Schema.Type;

public class JsonConverter<T extends GenericRecord> {
    private static final Logger LOG = LoggerFactory.getLogger(JsonConverter.class);

    private static final Set<Type> SUPPORTED_TYPES = ImmutableSet.of(Type.RECORD, Type.ARRAY, Type.MAP, Type.INT,
            Type.LONG, Type.BOOLEAN, Type.FLOAT, Type.DOUBLE, Type.STRING, Type.ENUM, Type.NULL);

    private final Class<T> typeClass;

    private final ObjectMapper mapper = new ObjectMapper();

    private final Schema baseSchema;

    private ConversionStats conversionStats = new ConversionStats();

    /**
     * Fails when there are json fields that are not in the schema.
     */
    private final boolean strictValidation;

    public JsonConverter(Schema schema, boolean strictValidation) {
        this(null, schema, strictValidation);
    }

    public JsonConverter(Schema schema) {
        this(null, schema, false);
    }

    public JsonConverter(Class<T> clazz, Schema schema, boolean strictValidation) {
        this.typeClass = clazz;
        this.baseSchema = validate(schema, true);
        this.strictValidation = strictValidation;
        assert SchemaBuilder.builder().booleanBuilder().endBoolean() != null;
    }

    private Schema validate(Schema schema, boolean mustBeRecord) {
        if (!mustBeRecord) {
            if (!SUPPORTED_TYPES.contains(schema.getType())) {
                throw new IllegalArgumentException("Unsupported type: " + schema.getType());
            }
            if (schema.getType() != Type.RECORD) {
                return schema;
            }
        }
        for (Field field : schema.getFields()) {
            Schema fs = field.schema();
            if (isNullableUnion(fs)) {
                fs = getNonNull(fs);
            }
            Type type = fs.getType();
            if (!SUPPORTED_TYPES.contains(type)) {
                throw new IllegalArgumentException(String.format("Unsupported type '%s' for field '%s'", type
                        .toString(), field.name()));
            }
            switch (type) {
                case RECORD:
                    validate(fs, true);
                    break;
                case MAP:
                    //trying to support options in map values
                    Schema fsVal = fs.getValueType();
                    if (isNullableUnion(fsVal)) {
                        fsVal = getNonNull(fsVal);
                    }
                    validate(fsVal, false);
                    break;
                case ARRAY:
                    validate(fs.getElementType(), false);
                default:
                    break;
            }
        }
        return schema;
    }

    @SuppressWarnings("unchecked")
    public T convert(String json) throws IOException {
        try {
            //let's be proactive and fail fast with strict validation
            Map<String, Object> fields = mapper.readValue(json, Map.class);
            if (strictValidation) performStrictValidation(fields, baseSchema);
            return convert(fields, baseSchema);
        } catch (IOException e) {
            throw new IOException("Failed to parse as Json: " + json + "\n\n" + e.getMessage());
        }
    }

    private void performStrictValidation(Map<String, Object> fields, Schema baseSchema) {
        Set<String> schemaFields = baseSchema.getFields().stream().map(f -> f.name()).collect(Collectors.toSet());
        Set<String> unknownFields = Sets.difference(fields.keySet(), schemaFields);
        if (unknownFields.size() > 0) throw new UndefinedFieldsException(unknownFields, baseSchema);
    }

    @SuppressWarnings("unchecked")
    private T convert(Map<String, Object> raw, Schema schema) throws IOException {
        GenericRecord result = typeClass == null ? new GenericData.Record(schema) : ClassUtil.createInstance
                (typeClass, false);
        Map<String, Object> clean = cleanAvro(raw);
        Set<String> usedFields = Sets.newHashSet();
        Set<String> missingFields = Sets.newHashSet();
        for (Field f : schema.getFields()) {
            String name = f.name();
            Object rawValue = clean.get(name);
            if (rawValue != null) {
                try {
                    result.put(f.pos(), typeConvert(rawValue, name, f.schema()));
                } catch (NumberFormatException e) {
                    throw new InvalidDataTypeException(name, rawValue, f.schema());
                }
                usedFields.add(name);
            } else if (f.schema().getType() == Type.NULL) {
                result.put(f.pos(), null);
                usedFields.add(name);
            } else {
                missingFields.add(name);
                JsonNode defaultValue = JacksonUtils.toJsonNode(f.defaultVal());
                if (defaultValue == null || defaultValue.isNull()) {
                    if (isNullableUnion(f.schema())) {
                        result.put(f.pos(), null);
                    } else {
                        throw new RequiredFieldMissingException(f.name(), schema);
                    }
                } else {
                    Schema fieldSchema = f.schema();
                    if (isNullableUnion(fieldSchema)) {
                        fieldSchema = getNonNull(fieldSchema);
                    }
                    Object value;
                    switch (fieldSchema.getType()) {
                        case BOOLEAN:
                            value = defaultValue.asBoolean();
                            break;
                        case DOUBLE:
                            value = defaultValue.asDouble();
                            break;
                        case FLOAT:
                            value = (float) defaultValue.asDouble();
                            break;
                        case INT:
                            value = defaultValue.asInt();
                            break;
                        case LONG:
                            value = defaultValue.asLong();
                            break;
                        case STRING:
                            value = defaultValue.asText();
                            break;
                        case MAP:
                            Map<String, Object> fieldMap = mapper.readValue(defaultValue.asText(), Map.class);
                            Map<String, Object> mvalue = Maps.newHashMap();
                            for (Map.Entry<String, Object> e : fieldMap.entrySet()) {
                                mvalue.put(e.getKey(), typeConvert(e.getValue(), name, fieldSchema.getValueType()));
                            }
                            value = mvalue;
                            break;
                        case ARRAY:
                            List fieldArray = mapper.readValue(defaultValue.asText(), List.class);
                            List lvalue = Lists.newArrayList();
                            for (Object elem : fieldArray) {
                                lvalue.add(typeConvert(elem, name, fieldSchema.getElementType()));
                            }
                            value = lvalue;
                            break;
                        case RECORD:
                            Map<String, Object> fieldRec = mapper.readValue(defaultValue.asText(), Map.class);
                            value = convert(fieldRec, fieldSchema);
                            break;
                        default:
                            throw new IllegalArgumentException("JsonConverter cannot handle type: " + fieldSchema
                                    .getType());
                    }
                    result.put(f.pos(), value);
                }
            }
        }

        if (usedFields.size() < raw.size()) {
            Set unknownFields = Sets.difference(raw.keySet(), usedFields);
            LOG.debug("There are unused JSON fields: " + Sets.difference(raw.keySet(), usedFields));
            conversionStats.reportUnknownFields(unknownFields);
        }

        conversionStats.reportMissingFields(missingFields);

        return (T) result;
    }

    @SuppressWarnings("unchecked")
    private Object typeConvert(Object value, String name, Schema schema) throws IOException {
        if (isNullableUnion(schema)) {
            if (value == null) {
                return null;
            } else {
                schema = getNonNull(schema);
            }
        } else if (value == null) {
            throw new JsonToAvroConversionException(value, name, schema);
        }

        switch (schema.getType()) {
            case BOOLEAN:
                if (value instanceof Boolean) {
                    return value;
                } else if (value instanceof String) {
                    return Boolean.valueOf((String) value);
                } else if (value instanceof Number) {
                    return ((Number) value).intValue() == 0 ? Boolean.FALSE : Boolean.TRUE;
                }
                break;
            case DOUBLE:
                if (value instanceof Number) {
                    return ((Number) value).doubleValue();
                } else if (value instanceof String) {
                    return Double.valueOf((String) value);
                }
                break;
            case FLOAT:
                if (value instanceof Number) {
                    return ((Number) value).floatValue();
                } else if (value instanceof String) {
                    return Float.valueOf((String) value);
                }
                break;
            case INT:
                if (value instanceof Number) {
                    return ((Number) value).intValue();
                } else if (value instanceof String) {
                    return Integer.valueOf((String) value);
                }
                break;
            case LONG:
                if (value instanceof Number) {
                    return ((Number) value).longValue();
                } else if (value instanceof String) {
                    return Long.valueOf((String) value);
                }
                break;
            case STRING:
                return value.toString();
            case ENUM:
                boolean valid = schema.getEnumSymbols().contains(value);
                if (!valid)
                    throw new IllegalArgumentException(value + " is not a valid symbol. Possible values are: " +
                            schema.getEnumSymbols() + ".");
                return new GenericData.EnumSymbol(schema, value);
            case RECORD:
                return convert((Map<String, Object>) value, schema);
            case ARRAY:
                Schema elementSchema = schema.getElementType();
                List listRes = new ArrayList();
                for (Object v : (List) value) {
                    listRes.add(typeConvert(v, name, elementSchema));
                }
                return listRes;
            case MAP:
                Schema valueSchema = schema.getValueType();
                Map<String, Object> mapRes = Maps.newHashMap();
                for (Map.Entry<String, Object> v : ((Map<String, Object>) value).entrySet()) {
                    mapRes.put(v.getKey(), typeConvert(v.getValue(), name, valueSchema));
                }
                return mapRes;
            default:
                throw new InvalidDataTypeException(name, value, schema);
        }
        throw new JsonToAvroConversionException(value, name, schema);
    }

    private boolean isNullableUnion(Schema schema) {
        return schema.getType().equals(Type.UNION) && schema.getTypes().size() == 2 && (schema.getTypes().get(0)
                .getType().equals(Type.NULL) || schema.getTypes().get(1).getType().equals(Type.NULL));
    }

    private Schema getNonNull(Schema schema) {
        List<Schema> types = schema.getTypes();
        return types.get(0).getType().equals(Type.NULL) ? types.get(1) : types.get(0);
    }


    public ConversionStats getConversionStats() {
        return conversionStats;
    }


    private Map cleanAvro(Map<String, Object> oldRaw) {
        Map<String, Object> newMap = Maps.newHashMap();
        try {
            oldRaw.forEach((entry, value) -> newMap.put(AvroUtils.cleanName(entry), value));
        } catch (Exception e) {
            e.printStackTrace();
        }
        return newMap;
    }
}