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

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;

import java.util.Set;
import java.util.stream.Collectors;

public class ConversionStats {

    enum FieldReportType {MISSING, NOT_IN_SCHEMA}

    private Multiset<FieldInfo> fields = HashMultiset.create();


    public void reportMissingFields(Set<String> missingFields) {
        if (!missingFields.isEmpty()) {
            for (String name : missingFields) {
                this.fields.add(new FieldInfo(FieldReportType.MISSING, name));
            }
        }
    }

    public void reportUnknownFields(Set<String> unknownFields) {
        if (!unknownFields.isEmpty()) {
            for (String name : unknownFields) {
                this.fields.add(new FieldInfo(FieldReportType.NOT_IN_SCHEMA, name));
            }
        }
    }


    public Set<FieldInfo> getMissingFields() {
        return fields.stream().filter(field -> field.getType() == FieldReportType.MISSING).collect(Collectors.toSet());
    }

    public int getMissingFieldsCount() {
        return getMissingFields().size();
    }

    public int getMissingFieldCount(String name) {
        int count = fields.count(new FieldInfo(FieldReportType.MISSING, name));
        return count;
    }

    public int getUnknownFieldCount(String name) {
        int count = fields.count(new FieldInfo(FieldReportType.NOT_IN_SCHEMA, name));
        return count;
    }

    public int getUnknownFieldsCount() {
        return getUnknownFields().size();
    }

    public Set<FieldInfo> getUnknownFields() {
        return fields.stream().filter(field -> field.getType() == FieldReportType.NOT_IN_SCHEMA).collect(Collectors
                .toSet());
    }

    public static class FieldInfo {

        private final String fieldName;

        private final FieldReportType type;

        public FieldInfo(FieldReportType type, String fieldName) {
            this.fieldName = fieldName;
            this.type = type;
        }

        public String getFieldName() {
            return fieldName;
        }

        public FieldReportType getType() {
            return type;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;

            FieldInfo fieldInfo = (FieldInfo) o;

            if (fieldName != null ? !fieldName.equals(fieldInfo.fieldName) : fieldInfo.fieldName != null) return false;
            return type == fieldInfo.type;
        }

        @Override
        public int hashCode() {
            int result = fieldName != null ? fieldName.hashCode() : 0;
            result = 31 * result + (type != null ? type.hashCode() : 0);
            return result;
        }
    }
}
