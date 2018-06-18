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

import com.fasterxml.jackson.databind.JsonNode;
import io.swagger.v3.core.util.Json;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.media.ArraySchema;
import io.swagger.v3.oas.models.media.Content;
import io.swagger.v3.oas.models.media.MediaType;
import io.swagger.v3.oas.models.media.Schema;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An enumerator for OpenAPI backed things.
 */
class OpenAPIEnumerator implements Enumerator<Object[]> {
  private static final String MEDIA_TYPE = "application/json";

  private final RowConverter<Object[]> rowConverter;
  private final Operation operation;

  private JsonNode root;
  private int rootIndex;
  private Object[] current;

  OpenAPIEnumerator(
      String sourceURL,
      AtomicBoolean cancelFlag,
      Schema entityModel,
      List<String> order,
      int[] fields,
      Operation operation
  ) {
    this.operation = operation;
    this.rowConverter = new ArrayRowConverter(entityModel, order, fields);
    try {
      Source source = Sources.of(Cache.getFile(sourceURL));
      this.root = Json.mapper().readTree(source.reader());
    } catch (IOException e) {
      this.root = null;
      e.printStackTrace();
    }
    this.rootIndex = 0;
    this.current = null;
  }

  public void close() {
    throw new UnsupportedOperationException();
  }

  @Override public Object[] current() {
    return current;
  }

  @Override public boolean moveNext() {
    Content content = operation.getResponses().get("200").getContent();
    MediaType mediaType = content.get(MEDIA_TYPE);

    if (mediaType == null) {
      throw new RuntimeException("Could not lookup media type " + MEDIA_TYPE);
    }

    Schema schema = mediaType.getSchema();
    if (schema instanceof ArraySchema) {
      if (rootIndex == root.size()) {
        current = null;
        return false;
      }
      JsonNode currentNode = root.get(rootIndex++);
      current = rowConverter.convertRow(currentNode);
      return true;
    } else {
      // assume it is a single object
     if (rootIndex == 0) {
        current = rowConverter.convertRow(root);
        rootIndex = 1;
        return true;
      }
      return false;
    }
  }

  public void reset() {
    throw new UnsupportedOperationException();
  }

  /**
   * Converts rows.
   * @param <E> Conversion target type.
   */
  abstract static class RowConverter<E> {
    abstract E convertRow(JsonNode node);

    protected Object convert(OpenAPIFieldType fieldType, JsonNode node) {
      if (fieldType == null) {
        return node;
      }
      if (node == null) {
        return null;
      }
      // TODO: Maybe add BOOLEAN and FLOAT?
      switch (fieldType) {
      case STRING:
        return node.asText();
      case INTEGER:
        return node.asInt();
      case OBJECT:
        return node.toString();
      case ARRAY:
        return node.toString();
      default:
        throw new UnsupportedOperationException("RowConverter called with unimplemented FieldType " + fieldType.toString());
      }
    }
  }

  /**
   * Array row converter.
   */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    private final Schema schema;
    private final List<String> order;
    private final int[] fields;

    ArrayRowConverter(Schema schema, List<String> order, int[] fields) {
      this.schema = schema;
      this.order = order;
      this.fields = fields;
    }

    @Override Object[] convertRow(JsonNode currentNode) {
      final Object[] objects = new Object[fields.length];

      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        final String fieldName = order.get(field);
        final Schema property = ((Schema) schema.getProperties().get(fieldName));
        final OpenAPIFieldType type = OpenAPIFieldType.of(property.getType());
        objects[i] = convert(type, currentNode.get(fieldName));
      }
      return objects;
    }
  }
}

// End OpenAPIEnumerator.java
