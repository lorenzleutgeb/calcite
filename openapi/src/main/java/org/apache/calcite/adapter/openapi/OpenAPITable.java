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

import io.swagger.v3.oas.models.OpenAPI;
import io.swagger.v3.oas.models.Operation;
import io.swagger.v3.oas.models.PathItem;
import io.swagger.v3.oas.models.media.Schema;
import io.swagger.v3.oas.models.parameters.Parameter;
import io.swagger.v3.oas.models.servers.Server;
import org.apache.calcite.DataContext;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.FilterableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Wraps OpenAPI result sets as tables.
 */
public class OpenAPITable<T> extends AbstractTable implements FilterableTable {
  private static final Logger LOGGER = LoggerFactory.getLogger(OpenAPITable.class);
  private static final String REFERENCE_PREFIX = "#/components/schemas/";

  private final OpenAPI api;
  private final String schemaKey;
  private final Schema schema;
  private final List<String> order;

  public OpenAPITable(OpenAPI api, String schemaKey) {
    this.api = api;
    this.schemaKey = schemaKey;
    this.schema = this.api.getComponents().getSchemas().get(this.schemaKey);

    @SuppressWarnings("unchecked")
    final Map<String, Schema> properties = (Map<String, Schema>) this.schema.getProperties();

    order = properties.keySet().stream().sorted().collect(Collectors.toList());
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final JavaTypeFactory jtypeFactory = (JavaTypeFactory) typeFactory;

    final List<String> fieldNames = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();
    for (String fieldName : order) {
      fieldNames.add(fieldName);
      Schema property = (Schema) schema.getProperties().get(fieldName);
      String typeString = property.getType();

      if (typeString == null) {
        final String ref = property.get$ref();
        if (ref.startsWith(REFERENCE_PREFIX)) {

          property = api.getComponents().getSchemas()
              .get(ref.substring(REFERENCE_PREFIX.length()));

          if (property == null) {
            throw new RuntimeException("ERROR: Unable to resolve reference: "
                + ref + " for column: " + fieldName);
          }
          typeString = property.getType();
        } else {
          throw new RuntimeException("ERROR: Unable to resolve reference: "
              + ref + " for column: " + fieldName
              + ". This is not your fault, the implementation is incomplete!");
        }
      }

      final OpenAPIFieldType fieldType = OpenAPIFieldType.of(typeString);
      if (fieldType == null) {
        throw new RuntimeException("ERROR: Found unknown type: "
          + typeString + " for column: " + fieldName);
      }
      types.add(fieldType.toType(jtypeFactory));
    }
    return jtypeFactory.createStructType(Pair.zip(fieldNames, types));
  }

  @Override public Enumerable<Object[]> scan(DataContext root, List<RexNode> filters) {
    if (filters.size() == 0) {
      throw new UnsupportedOperationException("Where clause is required.");
    }

    final RexNode firstFilter = filters.get(0);
    String pathKey = null;
    Map.Entry<Integer, String> mapping = null;

    if (firstFilter.isA(SqlKind.EQUALS)) {
      mapping = getMapping(firstFilter);
      if (mapping != null) {
        pathKey = matchPath(mapping.getKey());
      }
    } else if (firstFilter.isA(SqlKind.AND)) {
      for (RexNode operand : ((RexCall) firstFilter).operands) {
        Map.Entry<Integer, String> nextMapping = getMapping(operand);
        if (nextMapping == null) {
          continue;
        }

        String nextPathKey = matchPath(nextMapping.getKey());
        if (nextPathKey == null) {
          continue;
        }

        if (pathKey != null) {
          throw new RuntimeException(
              "Need exactly one filter that maps to an API path (have at least two)."
          );
        } else {
          mapping = nextMapping;
          pathKey = nextPathKey;
        }
      }
    } else {
      throw new RuntimeException("Where clause must be a single constraint or an AND.");
    }

    if (pathKey == null) {
      throw new RuntimeException("No matching path found!");
    }

    final String url = parameterize(pathKey, mapping.getKey(), mapping.getValue());

    final int[] fields = IntStream.range(0, order.size()).toArray();
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    final Operation getOperation = api.getPaths().get(pathKey).getGet();

    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new OpenAPIEnumerator<>(
            url,
            cancelFlag,
            schema,
            order,
            fields,
            getOperation
        );
      }
    };
  }

  private Map.Entry<Integer, String> getMapping(RexNode filter) {
    if (!filter.isA(SqlKind.EQUALS)) {
      LOGGER.warn("Invalid filter (need kind equals)");
      return null;
    }

    if (!(filter instanceof RexCall)) {
      LOGGER.warn("Invalid filter (need RexCall)");
      return null;
    }

    final List<RexNode> operands = Collections.unmodifiableList(((RexCall) filter).operands);

    // Only simple where clauses are handled
    if (operands.size() != 2) {
      LOGGER.warn("Invalid filter (need exactly two operands)");
      return null;
    }

    RexNode node = operands.get(0);
    if (!(node instanceof RexInputRef)) {
      LOGGER.warn("Invalid filter (need column on lhs)");
      return null;
    }
    final int key = ((RexInputRef) node).getIndex();

    node = operands.get(1);
    if (!(node instanceof RexLiteral)) {
      LOGGER.warn("Invalid filter (need literal on rhs)");
      return null;
    }
    final String value = encode((RexLiteral) node);

    return new AbstractMap.SimpleEntry<>(key, value);
  }

  /**
   * Attempts to match a given column to an OpenAPI declared path. The column is specified by its
   * index, and the matched path is specified by its key.
   * @param columnIndex the index of the column to match
   * @return the key of the path that has matched.
   */
  private String matchPath(int columnIndex) {
    final String columnName = order.get(columnIndex);

    for (Map.Entry<String, PathItem> entry : api.getPaths().entrySet()) {
      final Operation get = entry.getValue().getGet();
      if (get == null) {
        continue;
      }

      final List<Parameter> parameters = get.getParameters();
      // TODO: In case there are multiple parameters, choose or combine them somehow.
      if (parameters == null || parameters.size() != 1) {
        continue;
      }

      final String parameterName = parameters.get(0).getName();
      // FIXME: This is a heuristic that doesn't work for all APIs!
      // In queries like /pet/{petId}, the parameter {petId} is mapped to
      // "id" by removing the lowercased class name
      final String parameterNameWithoutClass = parameterName.startsWith(schemaKey.toLowerCase())
          ? lowerCaseFirst(parameterName.substring(schemaKey.length()))
          : "";

      if (parameterName.equals(columnName) || parameterNameWithoutClass.equals(columnName)) {
        return entry.getKey();
      }
    }

    LOGGER.warn("Column \"" + columnName + "\" not mapped to any path");
    return null;
  }

  private String parameterize(String pathKey, int columnIndex, String value) {
    final String base = url(pathKey);

    // Parameter is passed as part of the query string.
    if (!base.contains("{")) {
      final String columnName = order.get(columnIndex);
      return base + String.format("?%s=%s", columnName, value);
    }

    // Parameter is passed as part of the path.
    final PathItem pathItem = api.getPaths().get(pathKey);
    final String parameterName = pathItem.getGet().getParameters().get(0).getName();
    final String parameter = String.format("{%s}", parameterName);
    return base.replace(parameter, value);
  }

  private String encode(RexLiteral literal) {
    final String asString = literal.toString();
    final String typeName = literal.getTypeName().getName();
    if ("CHAR".equals(typeName)) {
      return asString.substring(1, asString.length() - 1);
    }
    return asString;
  }

  private String url(String path) {
    List<Server> servers = api.getServers();
    if (servers.isEmpty()) {
      throw new RuntimeException("No servers defined!");
    }
    if (servers.size() > 1) {
      throw new RuntimeException("More than one server define. Cannot handle this case!");
    }
    return servers.get(0).getUrl() + path;
  }

  private String lowerCaseFirst(String s) {
    if (s == null || s.isEmpty()) {
      throw new IllegalArgumentException("String to lowercase must not be empty.");
    }
    return s.substring(0, 1).toLowerCase() + s.substring(1);
  }
}

// End OpenAPITable.java
