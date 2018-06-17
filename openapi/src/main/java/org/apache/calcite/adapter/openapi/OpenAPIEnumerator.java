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
import io.swagger.models.ArrayModel;
import io.swagger.models.Model;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.properties.Property;
import io.swagger.util.Json;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.util.Source;
import org.apache.calcite.util.Sources;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * An enumerator for OpenAPI backed things.
 * @param <E> Some generic parameter.
 */
class OpenAPIEnumerator<E> implements Enumerator<E> {
  private final Model entityModel;
  private final List<String> order;
  private final int[] fields;
  private final RowConverter<E> rowConverter;
  private final Map.Entry<String, Path> sourcePath;

  private JsonNode root;
  private int rootIndex;
  private E current;

  OpenAPIEnumerator(
      String sourceURL,
      AtomicBoolean cancelFlag,
      Model entityModel,
      List<String> order,
      int[] fields,
      Map.Entry<String, Path> sourcePath
  ) {
    this.entityModel = entityModel;
    this.order = order;
    this.fields = fields;
    this.sourcePath = sourcePath;
    this.rowConverter = (RowConverter<E>) new ArrayRowConverter(entityModel, order, fields);
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

  @Override public E current() {
    return current;
  }

  @Override public boolean moveNext() {
    final Operation get = sourcePath.getValue().getGet();
    if (get.getResponses().get("200").getResponseSchema() instanceof ArrayModel) {
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
      switch (fieldType) {
      case STRING:
        return node.asText();
      case INTEGER:
        return node.asInt();
      case OBJECT:
        return node.toString();
      case ARRAY:
        return node.toString();
//                case BOOLEAN:
//                    return node.toString();

//               case FLOAT:
//                  if (string.length() == 0) {
//                      return null;
//                  }
//                  return Float.parseFloat(string);
      default:
        throw new UnsupportedOperationException();
      }
    }
  }

  /**
   * Array row converter.
   */
  static class ArrayRowConverter extends RowConverter<Object[]> {
    private final Model entityModel;
    private final List<String> order;
    private final int[] fields;

    ArrayRowConverter(Model entityModel, List<String> order, int[] fields) {
      this.entityModel = entityModel;
      this.order = order;
      this.fields = fields;
    }

    @Override Object[] convertRow(JsonNode currentNode) {
      final Object[] objects = new Object[fields.length];

      for (int i = 0; i < fields.length; i++) {
        int field = fields[i];
        String fieldName = order.get(field);
        final Property property = entityModel.getProperties().get(fieldName);
        final OpenAPIFieldType type = OpenAPIFieldType.of(property.getType());
        objects[i] = convert(type, currentNode.get(fieldName));
      }
      return objects;
    }
  }
}

// End OpenAPIEnumerator.java
