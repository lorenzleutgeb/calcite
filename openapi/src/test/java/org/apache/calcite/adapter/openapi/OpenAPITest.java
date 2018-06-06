package org.apache.calcite.adapter.openapi;

import junit.framework.TestCase;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.util.Util;
import org.junit.Assert;
import org.junit.Test;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

public class OpenAPITest extends TestCase {

    Connection connection;
    Statement statement;

    @Override
    protected void setUp() throws SQLException {
        final Properties properties = new Properties();
        connection = DriverManager.getConnection("jdbc:calcite:", properties);
        final CalciteConnection calciteConnection = connection.unwrap(
                CalciteConnection.class);

        final Schema testSchema = new OpenAPISchema("src/test/resources/openapi/schema.json");
        calciteConnection.getRootSchema().add("Test", testSchema);

        final Schema petStoreSchema = new OpenAPISchema("http://petstore.swagger.io/v2/swagger.json");
        calciteConnection.getRootSchema().add("PetStore", petStoreSchema);

        statement = connection.createStatement();
    }

    @Override
    protected void tearDown() throws SQLException {
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
    public void testAnd() throws SQLException {
        final String sql = "select * from \"PetStore\".\"Pet\" where \"status\" = 'pending' AND \"name\" = 'Wayne'";
        final String results = collect(statement.executeQuery(sql));
//        Assert.assertEquals("category=null; id=5092; name=Wayne; photoUrls=[\"1\"]; status=pending; tags=[null]", results);
        Assert.assertEquals("", results);
    }

    @Test
    public void testIsNull() throws SQLException {
        final String sql = "select * from \"PetStore\".\"Pet\" where \"status\" = 'pending' AND \"name\" is null";
        final String results = collect(statement.executeQuery(sql));
        Assert.assertEquals("", results);
    }

    @Test
    public void testGetSingleObject() throws SQLException {
        final String sql = "select \"id\", \"name\" from \"PetStore\".\"Pet\" where \"id\" = 109";
        final String results = collect(statement.executeQuery(sql));
        Assert.assertEquals("id=109; name=doggiepartha", results);
    }

    @Test
    public void testUser() throws SQLException {
        final String sql = "select \"userStatus\", \"username\" from \"PetStore\".\"User\" where \"username\" = 'user1'";
        final String results = collect(statement.executeQuery(sql));
        Assert.assertEquals("userStatus=0; username=user1", results);
    }

    @Test
    public void testInsert() {
        final String sql = "insert into \"PetStore\".\"Pet\" (\"name\", \"photoUrls\") values (\"user\", null)";
        try {
            statement.executeQuery(sql);
            Assert.fail();
        } catch (SQLException e) {
        }
    }

    @Test
    public void testUpdate() throws SQLException {
        final String sql = "update \"PetStore\".\"User\" set \"userStatus\" = 2 where \"username\" = 'user1'";
        try {
            statement.executeQuery(sql);
            Assert.fail();
        } catch (SQLException e) {
        }
    }

    private static int count(ResultSet resultSet) throws SQLException {
        int count = 0;
        while (resultSet.next())
            count++;
        return count;
    }

    private static String collect(ResultSet resultSet)
            throws SQLException {
        List<String> result = new ArrayList<>();
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
}
