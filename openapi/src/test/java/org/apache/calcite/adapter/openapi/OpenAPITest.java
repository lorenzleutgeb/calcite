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

import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.Util;

import com.google.common.base.Function;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * Tests for the OpenAPI adapter.
 */
public class OpenAPITest {

  private Connection connection;
  private Statement statement;

  private static int count(ResultSet resultSet) throws SQLException {
    int count = 0;
    while (resultSet.next()) {
      count++;
    }
    return count;
  }

  private static String collect(ResultSet resultSet)
      throws SQLException {
    List<String> result = new ArrayList<String>();
    final StringBuilder buf = new StringBuilder();
    while (resultSet.next()) {
      buf.setLength(0);
      int n = resultSet.getMetaData().getColumnCount();
      String sep = "";
      for (int i = 1; i <= n; i++) {
        buf.append(sep)
            .append(resultSet.getMetaData().getColumnLabel(i))
            .append("=")
            .append(resultSet.getString(i));
        sep = "; ";
      }
      result.add(Util.toLinux(buf.toString()));
    }
    return result.stream().collect(Collectors.joining(", "));
  }

  @Before
  public void setUp() throws SQLException {
    final Properties properties = new Properties();
    connection = DriverManager.getConnection("jdbc:calcite:", properties);
    final CalciteConnection calciteConnection = connection.unwrap(
        CalciteConnection.class);

    final Schema testSchema = new OpenAPISchema("src/test/resources/openapi/rainbow.yaml");
    calciteConnection.getRootSchema().add("Test", testSchema);

    final Schema petStoreSchema = new OpenAPISchema("src/test/resources/openapi/petstore.yaml");
    calciteConnection.getRootSchema().add("PetStore", petStoreSchema);

    statement = connection.createStatement();
  }

  @After
  public void tearDown() throws SQLException {
    connection.close();
  }

  @Test
  public void testQueryByInt() throws SQLException {
    final String sql = "select \"id\" from \"Test\".\"Rainbow\" where \"id\" = 0";
    final String results = collect(statement.executeQuery(sql));
    Assert.assertEquals("id=0", results);
  }

  @Test
  public void testQueryByString() throws SQLException {
    final String sql = "select \"id\" from \"Test\".\"Rainbow\" where \"a_string\" = 'str'";
    final String results = collect(statement.executeQuery(sql));
    Assert.assertEquals("id=0", results);
  }

  @Test
  public void testSimple() throws SQLException {
    final String sql = "select * from \"PetStore\".\"Pet\" where \"status\" = 'available'";
    final ResultSet resultSet = statement.executeQuery(sql);
    int rows = count(resultSet);
    Assert.assertEquals(true, rows > 0);
  }

  @Test
  public void testEmpty() throws SQLException {
    final String sql = "select * from \"PetStore\".\"Pet\" where \"id\" = 21798122";
    final ResultSet resultSet = statement.executeQuery(sql);
    int rows = count(resultSet);
    Assert.assertEquals(false, rows > 0);
  }

  @Test
  public void testAnd() throws SQLException {
    final String sql = "select * from \"PetStore\".\"Pet\" "
        + "where \"status\" = 'pending' AND \"name\" = 'Wayne'";
    final String results = collect(statement.executeQuery(sql));
    //Assert.assertEquals("category=null; id=5092; name=Wayne; photoUrls=[\"1\"];"
    // + " status=pending; tags=[null]", results);
    Assert.assertEquals("", results);
  }

  @Test
  public void testIsNull() throws SQLException {
    final String sql = "select * from \"PetStore\".\"Pet\" "
        + "where \"status\" = 'pending' AND \"name\" is null";
    final String results = collect(statement.executeQuery(sql));
    Assert.assertEquals("", results);
  }

  @Test
  public void testGetSingleObject() throws SQLException {
    final String sql = "select \"id\", \"name\" from \"PetStore\".\"Pet\" "
        + "where \"id\" = 109";
    final String results = collect(statement.executeQuery(sql));
    Assert.assertEquals("id=109; name=doggiepartha", results);
  }

  @Test
  public void testUser() throws SQLException {
    final String sql = "select \"userStatus\", \"username\" from \"PetStore\".\"User\" "
        + "where \"username\" = 'user1'";
    final String results = collect(statement.executeQuery(sql));
    Assert.assertEquals("userStatus=0; username=user1", results);
  }

  @Test
  public void testInsert() {
    final String sql =
        "insert into \"PetStore\".\"Pet\" (\"name\", \"photoUrls\") values (\"user\", null)";
    try {
      statement.executeQuery(sql);
      Assert.fail();
    } catch (SQLException e) {
    }
  }

  @Test
  public void testUpdate() throws SQLException {
    final String sql = "update \"PetStore\".\"User\" set \"userStatus\" = 2 where "
        + "\"username\" = 'user1'";
    try {
      statement.executeQuery(sql);
      Assert.fail();
    } catch (SQLException e) {
    }
  }

  @Test
  public void withModel() throws SQLException {
    final String sql = "select \"id\", \"name\" from \"PetStore\".\"Pet\" "
        + "where \"id\" = 109";

    checkSql(sql, "model", r -> {
      try {
        Assert.assertEquals("id=109; name=doggiepartha", collect(r));
      } catch (SQLException e) {
        Assert.fail();
      }
      return null;
    });
  }

  private void checkSql(String sql, String model, Function<ResultSet, Void> fn)
      throws SQLException {
    Connection connection = null;
    Statement statement = null;
    try {
      Properties info = new Properties();
      info.put("model", jsonPath(model));
      connection = DriverManager.getConnection("jdbc:calcite:", info);
      statement = connection.createStatement();
      final ResultSet resultSet =
          statement.executeQuery(
              sql);
      fn.apply(resultSet);
    } finally {
      close(connection, statement);
    }
  }

  private String jsonPath(String model) {
    return resourcePath(model + ".json");
  }

  private String resourcePath(String path) {
    final URL url = OpenAPITest.class.getResource("/" + path);
    // URL converts a space to %20, undo that.
    try {
      String s = URLDecoder.decode(url.toString(), "UTF-8");
      if (s.startsWith("file:")) {
        s = s.substring("file:".length());
      }
      return s;
    } catch (UnsupportedEncodingException e) {
      throw new RuntimeException(e);
    }
  }

  private void close(Connection connection, Statement statement) {
    if (statement != null) {
      try {
        statement.close();
      } catch (SQLException e) {
        // ignore
      }
    }
    if (connection != null) {
      try {
        connection.close();
      } catch (SQLException e) {
        // ignore
      }
    }
  }
}

// End OpenAPITest.java
