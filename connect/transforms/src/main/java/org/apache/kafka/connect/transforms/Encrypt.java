/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.connect.transforms;

import org.apache.kafka.common.cache.Cache;
import org.apache.kafka.common.cache.LRUCache;
import org.apache.kafka.common.cache.SynchronizedCache;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.data.Timestamp;
import org.apache.kafka.connect.transforms.util.SimpleConfig;
import org.apache.kafka.connect.transforms.util.SchemaUtil;

import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import static org.apache.kafka.connect.transforms.util.Requirements.requireMap;
import static org.apache.kafka.connect.transforms.util.Requirements.requireSinkRecord;
import static org.apache.kafka.connect.transforms.util.Requirements.requireStruct;

public abstract class Encrypt<R extends ConnectRecord<R>> implements Transformation<R> {

    public static final String OVERVIEW_DOC =
            "Encrypt the value of a record using AWS KMS.";



    private static final String OPTIONALITY_DOC = "Suffix with <code>!</code> to make this a required field, or <code>?</code> to keep it optional (the default).";

    public static final ConfigDef CONFIG_DEF = new ConfigDef()
            .define("topic", ConfigDef.Type.STRING, null, ConfigDef.Importance.MEDIUM,
                    "Field name for Kafka topic. " + OPTIONALITY_DOC)
            ;
    private String topic;
    @Override
    public void configure(Map<String, ?> props) {
        final SimpleConfig config = new SimpleConfig(CONFIG_DEF, props);
        topic = config.getString("topic");
    }

    @Override
    public R apply(R record) {
        /*Schema valueSchema = SchemaBuilder.struct()
                .name("value")
                .field("key_1", Schema.STRING_SCHEMA)
                .field("key_2", Schema.INT8_SCHEMA)
                .field("key_3", Schema.INT16_SCHEMA)
                .build();*/
        System.out.println(record.valueSchema());
        System.out.println(record.value());
        System.out.println(record.valueSchema().getClass());
        System.out.println(record.value().getClass());
        //{SSN=986119889,FIRST_NAME=Woodrow,LAST_NAME=Dudding,ACCOUNT_ID=0121819,BALANCE=0.00,ACCOUNT_TYPE=business,ADDRESS=61 Colonial St. Brooklyn NY 11201
        Struct struct = new Struct(record.valueSchema())
                .put("SSN","ssn")
                .put("FIRST_NAME","first_name")
                .put("LAST_NAME","last_name")
                .put("ACCOUNT_ID","account_id")
                .put("BALANCE","balance")
                .put("ACCOUNT_TYPE","account_type")
                .put("ADDRESS","address");
        return newRecord(record, record.valueSchema(), struct);
        /*if (operatingSchema(record) == null) {
            return applySchemaless(record);
        } else {
            return applyWithSchema(record);
        }*/
    }

    //No difference between schemaless records and records with schemas, so the logic is being moved to apply itself
    /*private R applySchemaless(R record) {
        final Map<String, Object> value = requireMap(operatingValue(record), PURPOSE);

        final Map<String, Object> updatedValue = new HashMap<>(value);

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            updatedValue.put(offsetField.name, requireSinkRecord(record, PURPOSE).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, record.timestamp());
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }
        final Map<String, Object> updatedValueMock = new HashMap<>(value);
        updatedValueMock.put("test_schemaless", "test_schemaless");
        return newRecord(record, null, updatedValueMock);
    }

    private R applyWithSchema(R record) {
        final Struct value = requireStruct(operatingValue(record), PURPOSE);

        Schema updatedSchema = schemaUpdateCache.get(value.schema());
        if (updatedSchema == null) {
            updatedSchema = makeUpdatedSchema(value.schema());
            schemaUpdateCache.put(value.schema(), updatedSchema);
        }

        final Struct updatedValue = new Struct(updatedSchema);

        for (Field field : value.schema().fields()) {
            updatedValue.put(field.name(), value.get(field));
        }

        if (topicField != null) {
            updatedValue.put(topicField.name, record.topic());
        }
        if (partitionField != null && record.kafkaPartition() != null) {
            updatedValue.put(partitionField.name, record.kafkaPartition());
        }
        if (offsetField != null) {
            updatedValue.put(offsetField.name, requireSinkRecord(record, PURPOSE).kafkaOffset());
        }
        if (timestampField != null && record.timestamp() != null) {
            updatedValue.put(timestampField.name, new Date(record.timestamp()));
        }
        if (staticField != null && staticValue != null) {
            updatedValue.put(staticField.name, staticValue);
        }
        final Struct updatedValueMock = new Struct(updatedSchema);
        updatedValueMock.put("test_withschema","test_withschema");
        return newRecord(record, updatedSchema, updatedValueMock);
    }*/

    //No changes to the schema for enryption, so this is not needed
    /*private Schema makeUpdatedSchema(Schema schema) {
        final SchemaBuilder builder = SchemaUtil.copySchemaBasics(schema, SchemaBuilder.struct());

        for (Field field : schema.fields()) {
            builder.field(field.name(), field.schema());
        }

        if (topicField != null) {
            builder.field(topicField.name, topicField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        }
        if (partitionField != null) {
            builder.field(partitionField.name, partitionField.optional ? Schema.OPTIONAL_INT32_SCHEMA : Schema.INT32_SCHEMA);
        }
        if (offsetField != null) {
            builder.field(offsetField.name, offsetField.optional ? Schema.OPTIONAL_INT64_SCHEMA : Schema.INT64_SCHEMA);
        }
        if (timestampField != null) {
            builder.field(timestampField.name, timestampField.optional ? OPTIONAL_TIMESTAMP_SCHEMA : Timestamp.SCHEMA);
        }
        if (staticField != null) {
            builder.field(staticField.name, staticField.optional ? Schema.OPTIONAL_STRING_SCHEMA : Schema.STRING_SCHEMA);
        }

        return builder.build();
    }*/

    @Override
    public void close() {
        //schemaUpdateCache = null;
    }

    @Override
    public ConfigDef config() {
        return CONFIG_DEF;
    }

    //This is not needed for encryption, since only the value will be encrypted
    //protected abstract Schema operatingSchema(R record);

    protected abstract Object operatingValue(R record);

    protected abstract R newRecord(R record, Schema updatedSchema, Object updatedValue);

    public static class Key<R extends ConnectRecord<R>> extends Encrypt<R> {

        //This is not needed for encryption, since only the value will be encrypted
        /*@Override
        protected Schema operatingSchema(R record) {
            return record.keySchema();
        }*/

        @Override
        protected Object operatingValue(R record) {
            return record.key();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), updatedSchema, updatedValue, record.valueSchema(), record.value(), record.timestamp());
        }

    }

    public static class Value<R extends ConnectRecord<R>> extends Encrypt<R> {

    //This is not needed for encryption, since only the value will be encrypted
        /*@Override
        protected Schema operatingSchema(R record) {
            return record.valueSchema();
        }*/

        @Override
        protected Object operatingValue(R record) {
            return record.value();
        }

        @Override
        protected R newRecord(R record, Schema updatedSchema, Object updatedValue) {
            return record.newRecord(record.topic(), record.kafkaPartition(), record.keySchema(), record.key(), updatedSchema, updatedValue, record.timestamp());
        }

    }

}
