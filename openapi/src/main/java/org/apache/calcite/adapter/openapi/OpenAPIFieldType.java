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

import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.linq4j.tree.Primitive;
import org.apache.calcite.rel.type.RelDataType;

import java.util.HashMap;
import java.util.Map;

/**
 * @link https://github.com/OAI/OpenAPI-Specification/blob/master/versions/3.0.0.md#dataTypes
 */
enum OpenAPIFieldType {
  INTEGER(java.lang.Integer.class, "integer"),
  //    LONG(java.lang.Integer, "integer"),
  FLOAT(java.lang.Float.class, "number"),
  DOUBLE(java.lang.Double.class, "double"),
  STRING(String.class, "string"),
  BOOLEAN(Primitive.BOOLEAN),
  //    BYTE(java.lang.Byte.class, "string"),
//    DATE(java.sql.Date.class, "date"),
  DATETIME(java.sql.Timestamp.class, "date"),
//    PASSWORD(String.class, "string"),

  ARRAY(java.sql.Array.class, "array"),
  OBJECT(java.lang.Object.class, "ref");

  private static final Map<String, OpenAPIFieldType> MAP = new HashMap<>();

  static {
    for (OpenAPIFieldType value : values()) {
      MAP.put(value.simpleName, value);
    }
  }

  private final Class clazz;
  private final String simpleName;

  OpenAPIFieldType(Primitive primitive) {
    this(primitive.boxClass, primitive.primitiveClass.getSimpleName());
  }

  OpenAPIFieldType(Class clazz, String simpleName) {
    this.clazz = clazz;
    this.simpleName = simpleName;
  }

  public static OpenAPIFieldType of(String typeString) {
    return MAP.get(typeString);
  }

  public RelDataType toType(JavaTypeFactory typeFactory) {
    RelDataType javaType = typeFactory.createJavaType(clazz);
    RelDataType sqlType = typeFactory.createSqlType(javaType.getSqlTypeName());
    return typeFactory.createTypeWithNullability(sqlType, true);
  }
}

// End OpenAPIFieldType.java
