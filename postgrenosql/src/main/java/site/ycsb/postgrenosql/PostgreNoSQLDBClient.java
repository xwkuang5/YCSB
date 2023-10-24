/*
 * Copyright 2017 YCSB Contributors. All Rights Reserved.
 *
 * CODE IS BASED ON the jdbc-binding JdbcDBClient class.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */
package site.ycsb.postgrenosql;

import site.ycsb.*;
import org.json.simple.JSONObject;
import org.postgresql.Driver;
import org.postgresql.util.PGobject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import site.ycsb.generator.NumberGenerator;
import site.ycsb.generator.UniformLongGenerator;
import site.ycsb.workloads.DocumentWorkload;

/**
 * PostgreNoSQL client for YCSB framework.
 */
public class PostgreNoSQLDBClient extends DB {

  private static final Logger LOG = LoggerFactory.getLogger(PostgreNoSQLDBClient.class);

  /**
   * The class to use as the jdbc driver.
   */
  public static final String DRIVER_CLASS = "db.driver";

  /**
   * The URL to connect to the database.
   */
  public static final String CONNECTION_URL = "postgrenosql.url";

  /**
   * The user name to use to connect to the database.
   */
  public static final String CONNECTION_USER = "postgrenosql.user";

  /**
   * The password to use for establishing the connection.
   */
  public static final String CONNECTION_PASSWD = "postgrenosql.passwd";

  /**
   * The JDBC connection auto-commit property for the driver.
   */
  public static final String JDBC_AUTO_COMMIT = "postgrenosql.autocommit";

  /**
   * The transaction isolation level.
   */
  public static final String ISOLATION_LEVEL = "postgrenosql.isolationlevel";

  enum ScanType {
    TABLE_SCAN,
    SINGLE_FIELD_NUMERIC_SCAN;
  }

  public static final String QUERY_TYPE = "postgrenosql.scantype";

  public static final String QUERY_TYPE_DEFAULT = "tablescan";

  /**
   * The primary key in the user table.
   */
  public static final String PRIMARY_KEY = "YCSB_KEY";

  /**
   * The field name prefix in the table.
   */
  public static final String COLUMN_NAME = "YCSB_VALUE";

  private static final String DEFAULT_PROP = "";

  private static final String DEFAULT_ISOLATION_LEVEL = "READ_COMMITTED";

  /**
   * Cache for already prepared statements.
   */
  private Map<StatementType, PreparedStatement> cachedStatements;

  /**
   * The driver to get the connection to postgresql.
   */
  private Driver postgrenosqlDriver;

  /**
   * The connection to the database.
   */
  private Connection connection;


  private ScanType queryType;

  private NumberGenerator singleFieldNumericScanNumberGenerator;

  /**
   * Returns parsed boolean value from the properties if set, otherwise returns defaultVal.
   */
  private static boolean getBoolProperty(Properties props, String key, boolean defaultVal) {
    String valueStr = props.getProperty(key);
    if (valueStr != null) {
      return Boolean.parseBoolean(valueStr);
    }
    return defaultVal;
  }

  private static int toIsolationLevel(String isolationLevel) {
    switch (isolationLevel) {
      case "READ_COMMITTED":
        return Connection.TRANSACTION_READ_COMMITTED;
      case "REPEATABLE_READ":
        return Connection.TRANSACTION_REPEATABLE_READ;
      case "SERIALIZABLE":
        return Connection.TRANSACTION_SERIALIZABLE;
      default:
        throw new UnsupportedOperationException(
            String.format("Unsupported isolation level: %s", isolationLevel));
    }
  }

  @Override
  public void init() throws DBException {
    if (postgrenosqlDriver != null) {
      return;
    }

    Properties props = getProperties();
    String urls = props.getProperty(CONNECTION_URL, DEFAULT_PROP);
    String user = props.getProperty(CONNECTION_USER, DEFAULT_PROP);
    String passwd = props.getProperty(CONNECTION_PASSWD, DEFAULT_PROP);
    int isolationLevel = toIsolationLevel(
        props.getProperty(ISOLATION_LEVEL, DEFAULT_ISOLATION_LEVEL));
    boolean autoCommit = getBoolProperty(props, JDBC_AUTO_COMMIT, true);
    queryType = toScanType(props.getProperty(QUERY_TYPE, QUERY_TYPE_DEFAULT));
    singleFieldNumericScanNumberGenerator = new UniformLongGenerator(DocumentWorkload.NUMERIC_MIN,
        DocumentWorkload.NUMERIC_MAX);

    try {
      Properties tmpProps = new Properties();
      tmpProps.setProperty("user", user);
      tmpProps.setProperty("password", passwd);

      cachedStatements = new HashMap<>();

      postgrenosqlDriver = new Driver();
      connection = postgrenosqlDriver.connect(urls, tmpProps);
      connection.setTransactionIsolation(isolationLevel);
      connection.setAutoCommit(autoCommit);

    } catch (Exception e) {
      LOG.error("Error during initialization: " + e);
    }
  }

  private static ScanType toScanType(String scanType) {
    switch (scanType) {
      case "tablescan":
        return ScanType.TABLE_SCAN;
      case "singlefieldnumericscan":
        return ScanType.SINGLE_FIELD_NUMERIC_SCAN;
      default:
        throw new UnsupportedOperationException(String.format("Unsupported scan type: %s", scanType)
        );
    }
  }

  @Override
  public void cleanup() throws DBException {
    try {
      cachedStatements.clear();

      if (!connection.getAutoCommit()) {
        connection.commit();
      }
      connection.close();
    } catch (SQLException e) {
      System.err.println("Error in cleanup execution. " + e);
    }
    postgrenosqlDriver = null;
  }

  @Override
  public Status read(String tableName, String key, Set<String> fields,
      Map<String, ByteIterator> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.READ, tableName, fields);
      PreparedStatement readStatement = cachedStatements.get(type);
      if (readStatement == null) {
        readStatement = createAndCacheReadStatement(type);
      }
      readStatement.setString(1, key);
      ResultSet resultSet = readStatement.executeQuery();
      if (!resultSet.next()) {
        resultSet.close();
        return Status.NOT_FOUND;
      }

      if (result != null) {
        if (fields == null) {
          do {
            String field = resultSet.getString(2);
            String value = resultSet.getString(3);
            if (field.contains(DocumentWorkload.NUMERIC_FIELD_PREFIX)) {
              result.put(field, new NumericByteIterator(Long.parseLong(value)));
            } else {
              result.put(field, new StringByteIterator(value));
            }
          } while (resultSet.next());
        } else {
          for (String field : fields) {
            String value = resultSet.getString(field);
            if (field.contains(DocumentWorkload.NUMERIC_FIELD_PREFIX)) {
              result.put(field, new NumericByteIterator(Long.parseLong(value)));
            } else {
              result.put(field, new StringByteIterator(value));
            }
          }
        }
      }
      resultSet.close();
      return Status.OK;

    } catch (SQLException e) {
      LOG.error("Error in processing read of table " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status scan(String tableName, String startKey, int recordcount, Set<String> fields,
      Vector<HashMap<String, ByteIterator>> result) {
    try {
      StatementType type = new StatementType(StatementType.Type.SCAN, tableName, fields);
      PreparedStatement scanStatement = cachedStatements.get(type);
      if (scanStatement == null) {
        scanStatement = createAndCacheScanStatement(type);
      }
      switch (queryType) {
        case TABLE_SCAN:
          scanStatement.setString(1, startKey);
          break;
        case SINGLE_FIELD_NUMERIC_SCAN:
          scanStatement.setLong(1,
              singleFieldNumericScanNumberGenerator.nextValue().longValue()
          );
          break;
        default:
          throw new AssertionError("impossible");
      }
      scanStatement.setInt(2, recordcount);
      ResultSet resultSet = scanStatement.executeQuery();
      for (int i = 0; i < recordcount && resultSet.next(); i++) {
        if (result != null && fields != null) {
          HashMap<String, ByteIterator> values = new HashMap<String, ByteIterator>();
          for (String field : fields) {
            String value = resultSet.getString(field);
            if (field.contains(DocumentWorkload.NUMERIC_FIELD_PREFIX)) {
              values.put(field, new NumericByteIterator(Long.parseLong(value)));
            } else {
              values.put(field, new StringByteIterator(value));
            }
          }

          result.add(values);
        }
      }

      resultSet.close();
      return Status.OK;
    } catch (SQLException e) {
      LOG.error("Error in processing scan of table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status update(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      StatementType type = new StatementType(StatementType.Type.UPDATE, tableName, null);
      PreparedStatement updateStatement = cachedStatements.get(type);
      if (updateStatement == null) {
        updateStatement = createAndCacheUpdateStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        if (entry.getKey().contains(DocumentWorkload.NUMERIC_FIELD_PREFIX)) {
          jsonObject.put(entry.getKey(), Utils.bytesToLong(entry.getValue().toArray()));
        } else {
          jsonObject.put(entry.getKey(), entry.getValue().toString());
        }
      }

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      updateStatement.setObject(1, object);
      updateStatement.setString(2, key);

      int result = updateStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }
      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing update to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status insert(String tableName, String key, Map<String, ByteIterator> values) {
    try {
      StatementType type = new StatementType(StatementType.Type.INSERT, tableName, null);
      PreparedStatement insertStatement = cachedStatements.get(type);
      if (insertStatement == null) {
        insertStatement = createAndCacheInsertStatement(type);
      }

      JSONObject jsonObject = new JSONObject();
      for (Map.Entry<String, ByteIterator> entry : values.entrySet()) {
        if (entry.getKey().contains(DocumentWorkload.NUMERIC_FIELD_PREFIX)) {
          jsonObject.put(entry.getKey(), Utils.bytesToLong(entry.getValue().toArray()));
        } else {
          jsonObject.put(entry.getKey(), entry.getValue().toString());
        }
      }

      PGobject object = new PGobject();
      object.setType("jsonb");
      object.setValue(jsonObject.toJSONString());

      insertStatement.setObject(2, object);
      insertStatement.setString(1, key);

      int result = insertStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing insert to table: " + tableName + ": " + e);
      return Status.ERROR;
    }
  }

  @Override
  public Status delete(String tableName, String key) {
    try {
      StatementType type = new StatementType(StatementType.Type.DELETE, tableName, null);
      PreparedStatement deleteStatement = cachedStatements.get(type);
      if (deleteStatement == null) {
        deleteStatement = createAndCacheDeleteStatement(type);
      }
      deleteStatement.setString(1, key);

      int result = deleteStatement.executeUpdate();
      if (result == 1) {
        return Status.OK;
      }

      return Status.UNEXPECTED_STATE;
    } catch (SQLException e) {
      LOG.error("Error in processing delete to table: " + tableName + e);
      return Status.ERROR;
    }
  }

  private PreparedStatement createAndCacheReadStatement(StatementType readType)
      throws SQLException {
    PreparedStatement readStatement = connection.prepareStatement(createReadStatement(readType));
    PreparedStatement statement = cachedStatements.putIfAbsent(readType, readStatement);
    if (statement == null) {
      return readStatement;
    }
    return statement;
  }

  private String createReadStatement(StatementType readType) {
    StringBuilder read = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);

    if (readType.getFields() == null) {
      read.append(", (jsonb_each_text(" + COLUMN_NAME + ")).*");
    } else {
      for (String field : readType.getFields()) {
        read.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }

    read.append(" FROM " + readType.getTableName());
    read.append(" WHERE ");
    read.append(PRIMARY_KEY);
    read.append(" = ");
    read.append("?");
    return read.toString();
  }

  private PreparedStatement createAndCacheScanStatement(StatementType scanType)
      throws SQLException {
    PreparedStatement scanStatement = connection.prepareStatement(createScanStatement(scanType));
    PreparedStatement statement = cachedStatements.putIfAbsent(scanType, scanStatement);
    if (statement == null) {
      return scanStatement;
    }
    return statement;
  }

  private String createScanStatement(StatementType scanType) {
    StringBuilder scan = new StringBuilder("SELECT " + PRIMARY_KEY + " AS " + PRIMARY_KEY);
    if (scanType.getFields() != null) {
      for (String field : scanType.getFields()) {
        scan.append(", " + COLUMN_NAME + "->>'" + field + "' AS " + field);
      }
    }
    scan.append(" FROM " + scanType.getTableName());
    scan.append(" WHERE ");
    switch (queryType) {
      case TABLE_SCAN:
        scan.append(PRIMARY_KEY);
        scan.append(" >= ?");
        scan.append(" ORDER BY ");
        scan.append(PRIMARY_KEY);
        break;
      case SINGLE_FIELD_NUMERIC_SCAN:
        scan.append("CAST( " + COLUMN_NAME + "->'field_1_numeric' AS int8) >= ?");
        break;
      default:
        throw new AssertionError("impossible");
    }
    scan.append(" LIMIT ?");

    String result = scan.toString();
    LOG.info("Created scan statement: " + result);
    return result;
  }

  public PreparedStatement createAndCacheUpdateStatement(StatementType updateType)
      throws SQLException {
    PreparedStatement updateStatement = connection.prepareStatement(
        createUpdateStatement(updateType));
    PreparedStatement statement = cachedStatements.putIfAbsent(updateType, updateStatement);
    if (statement == null) {
      return updateStatement;
    }
    return statement;
  }

  private String createUpdateStatement(StatementType updateType) {
    StringBuilder update = new StringBuilder("UPDATE ");
    update.append(updateType.getTableName());
    update.append(" SET ");
    update.append(COLUMN_NAME + " = " + COLUMN_NAME);
    update.append(" || ? ");
    update.append(" WHERE ");
    update.append(PRIMARY_KEY);
    update.append(" = ?");
    return update.toString();
  }

  private PreparedStatement createAndCacheInsertStatement(StatementType insertType)
      throws SQLException {
    PreparedStatement insertStatement = connection.prepareStatement(
        createInsertStatement(insertType));
    PreparedStatement statement = cachedStatements.putIfAbsent(insertType, insertStatement);
    if (statement == null) {
      return insertStatement;
    }
    return statement;
  }

  private String createInsertStatement(StatementType insertType) {
    StringBuilder insert = new StringBuilder("INSERT INTO ");
    insert.append(insertType.getTableName());
    insert.append(" (" + PRIMARY_KEY + "," + COLUMN_NAME + ")");
    insert.append(" VALUES(?,?)");
    return insert.toString();
  }

  private PreparedStatement createAndCacheDeleteStatement(StatementType deleteType)
      throws SQLException {
    PreparedStatement deleteStatement = connection.prepareStatement(
        createDeleteStatement(deleteType));
    PreparedStatement statement = cachedStatements.putIfAbsent(deleteType, deleteStatement);
    if (statement == null) {
      return deleteStatement;
    }
    return statement;
  }

  private String createDeleteStatement(StatementType deleteType) {
    StringBuilder delete = new StringBuilder("DELETE FROM ");
    delete.append(deleteType.getTableName());
    delete.append(" WHERE ");
    delete.append(PRIMARY_KEY);
    delete.append(" = ?");
    return delete.toString();
  }
}
