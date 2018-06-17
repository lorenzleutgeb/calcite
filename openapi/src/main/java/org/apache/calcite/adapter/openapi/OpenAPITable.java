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

import com.google.common.collect.ImmutableList;
import io.swagger.models.Model;
import io.swagger.models.Operation;
import io.swagger.models.Path;
import io.swagger.models.Swagger;
import io.swagger.models.parameters.Parameter;
import io.swagger.models.properties.Property;
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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Wraps OpenAPI result sets as tables.
 */
public class OpenAPITable extends AbstractTable implements FilterableTable {
  private final Swagger swagger;
  private final String entity;
  private final Model entityModel;
  private final ArrayList<String> order;
  private String sourceURL;
  private Map.Entry<String, Path> sourcePath;

  public OpenAPITable(Swagger swagger, String entity) {
    this.swagger = swagger;
    this.sourceURL = null;
    this.sourcePath = null;
    this.entity = entity;
    this.entityModel = this.swagger.getDefinitions().get(this.entity);
    final Map<String, Property> properties = this.entityModel.getProperties();
    order = properties.keySet().stream().sorted().collect(Collectors.toCollection(ArrayList::new));
  }

  @Override public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    final JavaTypeFactory jtypeFactory = (JavaTypeFactory) typeFactory;
    final List<String> fieldNames = new ArrayList<>();
    final List<RelDataType> types = new ArrayList<>();
    for (int i = 0; i < order.size(); i++) {
      String fieldName = order.get(i);
      fieldNames.add(fieldName);

      final String typeString = entityModel.getProperties().get(fieldName).getType();
      final OpenAPIFieldType fieldType = OpenAPIFieldType.of(typeString);
      final RelDataType type;
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
    RexNode firstFilter = filters.get(0);
    boolean ok;
    long count = 0;
    if (firstFilter.isA(SqlKind.EQUALS)) {
      ok = findSourcePath(firstFilter);
    } else if (firstFilter.isA(SqlKind.AND)) {
      count = ((RexCall) firstFilter).operands.stream()
          .filter(filter -> findSourcePath(filter))
          .count();
      ok = count == 1;
    } else {
      throw new RuntimeException("Where clause must be a single constraint or an AND.");
    }
    if (!ok) {
      throw new RuntimeException(
          String.format("Need exactly one filter that maps to an API path (have %d).", count)
      );
    }
    assert sourceURL != null;
    final int[] fields = IntStream.range(0, order.size()).toArray();
    final AtomicBoolean cancelFlag = DataContext.Variable.CANCEL_FLAG.get(root);
    return new AbstractEnumerable<Object[]>() {
      public Enumerator<Object[]> enumerator() {
        return new OpenAPIEnumerator<>(
            sourceURL,
            cancelFlag,
            entityModel,
            order,
            fields,
            sourcePath
        );
      }
    };
  }

  private boolean findSourcePath(RexNode filter) {
    if (!filter.isA(SqlKind.EQUALS)) {
      return false;
    }
    assert filter instanceof RexCall;
    final ImmutableList<RexNode> operands = ((RexCall) filter).operands;
    // only simple where clauses are handled
    if (operands.size() != 2) {
      return false;
    }
    final RexNode columnNode = operands.get(0);
    if (!(columnNode instanceof RexInputRef)) {
//            throw new UnsupportedOperationException("Invalid filter (need column on lhs)");
      return false;
    }
    final RexNode inputNode = operands.get(1);
    if (!(inputNode instanceof RexLiteral)) {
//            throw new UnsupportedOperationException("Invalid filter (need literal on rhs)");
      return false;
    }
    String value = encode((RexLiteral) inputNode);
    int columnIndex = ((RexInputRef) columnNode).getIndex();
    String columnName = order.get(columnIndex);
    final Optional<Map.Entry<String, Path>> optionalPathEntry = swagger.getPaths().entrySet()
        .stream()
        .filter(entry -> {
          final Operation get = entry.getValue().getGet();
          if (get == null) {
            return false;
          }
          final List<Parameter> parameters = get.getParameters();
          if (parameters == null || parameters.size() != 1) {
            return false;
          }
          String parameterName = parameters.get(0).getName();
          // FIXME: this is a heuristic that doesn't work for all APIs
          // in queries like /pet/{petId}
          // {petId} is mapped to "id" by removing the lowercased class name
          String parameterNameWithoutClass = parameterName.startsWith(entity.toLowerCase())
              ? lowerCaseFirst(parameterName.substring(entity.length()))
              : "";
          return parameterName.equals(columnName) || parameterNameWithoutClass.equals(columnName);
        })
        .findFirst();

    if (!optionalPathEntry.isPresent()) {
//            throw new RuntimeException("Column \"" + columnName + "\" not mapped to any path");
      return false;
    }
    final Map.Entry<String, Path> pathEntry = optionalPathEntry.get();
    sourcePath = optionalPathEntry.get();
    String parameterName = sourcePath.getValue().getGet().getParameters().get(0).getName();
    String parameter = String.format("{%s}", parameterName);
    final String path = sourcePath.getKey();
    sourceURL = url(path);
    if (sourceURL.contains("{")) {
      sourceURL = sourceURL.replace(parameter, value);
    } else {
      sourceURL += String.format("?%s=%s", columnName, value);
    }

    return true;
  }

  String encode(RexLiteral literal) {
    String asString = literal.toString();
    String typeName = literal.getTypeName().getName();
    if (typeName.equals("CHAR")) {
      return asString.substring(1, asString.length() - 1);
    }
    return asString;
  }

  boolean isLocalFile() {
    return swagger.getSchemes() == null;
  }

  String url(String path) {
    final String prefix = isLocalFile() ? "" : String.format("%s://", swagger.getSchemes().get(0));
    return prefix + swagger.getHost() + swagger.getBasePath() + path;
  }

  private String lowerCaseFirst(String s) {
    if (s.isEmpty()) {
      throw new IllegalArgumentException("Input must not be empty.");
    }
    return s.substring(0, 1).toLowerCase() + s.substring(1);
  }
}

// End OpenAPITable.java
