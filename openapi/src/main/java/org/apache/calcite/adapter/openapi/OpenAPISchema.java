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

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import com.google.common.collect.ImmutableMap;

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.parser.OpenAPIV3Parser;

import java.io.File;
import java.util.Map;

/**
 * OpenAPI Schema
 */
public class OpenAPISchema extends AbstractSchema {
  private final OpenAPI swagger;
  private Map<String, Table> tableMap;

  public OpenAPISchema(String schemaURL) {
    super();
    final File schemaFile = Cache.getFile(schemaURL);
    this.swagger = new OpenAPIV3Parser().read(schemaFile.toString());
    if (swagger == null) {
      throw new RuntimeException("Swagger could not parse schema file " + schemaFile.toString());
    }
  }

  @Override protected Map<String, Table> getTableMap() {
    if (tableMap == null) {
      tableMap = createTableMap();
    }
    return tableMap;
  }

  private Map<String, Table> createTableMap() {
    final ImmutableMap.Builder<String, Table> builder = ImmutableMap.builder();
    swagger.getComponents().getSchemas().keySet().forEach(
        name -> builder.put(name, new OpenAPITable(swagger, name))
    );
    return builder.build();
  }
}

// End OpenAPISchema.java
