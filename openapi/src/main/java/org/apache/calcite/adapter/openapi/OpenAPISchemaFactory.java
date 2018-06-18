/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.adapter.openapi;

import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

/**
 * Factory that creates an {@link OpenAPISchemaFactory}.
 *
 * <p>Allows a custom schema to be included in a model.json file.
 *
 * !connect jdbc:calcite:schemaFactory=org.apache.calcite.adapter.openapi.OpenAPISchemaFactory
 */
@SuppressWarnings("UnusedDeclaration")
public class OpenAPISchemaFactory implements SchemaFactory {

  static {
    // TODO: Remove this once the issue is settled upstream.
    System.err.println(
        "NOTE: In case you are seeing a warning about illegal reflective access in "
            + "com.google.protobuf.UnsafeUtil please refer to "
            + "https://github.com/google/protobuf/issues/3781"
    );
  }

  public OpenAPISchemaFactory() {
  }

  @Override public Schema create(SchemaPlus parentSchema, String name,
                                 Map<String, Object> operand) {

    final ObjectMapper mapper = new ObjectMapper();
    mapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);

    final String schema = (String) operand.get("spec");
    return new OpenAPISchema(schema);
  }
}

// End OpenAPISchemaFactory.java
