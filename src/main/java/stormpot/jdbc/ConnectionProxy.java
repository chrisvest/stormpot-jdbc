package stormpot.jdbc;

import static stormpot.jdbc.StormpotDataSource.NOT_WRAPPED;

import java.sql.Array;
import java.sql.Blob;
import java.sql.CallableStatement;
import java.sql.ClientInfoStatus;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLNonTransientException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import stormpot.Poolable;
import stormpot.Slot;

class ConnectionProxy implements Poolable, Connection {
  private static final String CLOSED_MESSAGE = "The connection is closed.";

  private final Slot slot;
  private final Connection con;
  
  // This field is unprotected because a ConnectionProxy is, by virtue of the
  // pool, only ever accessed by a single thread at a time.
  // Connections are not promised to be thread-safe anyway.
  private boolean isClosed;

  public ConnectionProxy(Slot slot, Connection con) {
    if (slot == null) {
      throw new IllegalArgumentException("The slot parameter cannot be null.");
    }
    if (con == null) {
      throw new IllegalArgumentException("The con parameter cannot be null.");
    }
    this.slot = slot;
    this.con = con;
  }
  
  void closeDelegateConnection() throws SQLException {
    con.close();
  }

  @Override
  public void release() {
    isClosed = true;
    slot.release(this);
  }

  @Override
  public void close() throws SQLException {
    if (!isClosed) {
      release();
    }
  }
  
  void reopen() {
    isClosed = false;
  }

  @Override
  public boolean isClosed() throws SQLException {
    return isClosed;
  }
  
  private void assertNotClosed() throws SQLException {
    if (isClosed) {
      throw newConnectionClosedException();
    }
  }

  private SQLNonTransientException newConnectionClosedException() {
    return new SQLNonTransientException(CLOSED_MESSAGE);
  }

  @Override
  public boolean isValid(int timeout) throws SQLException {
    // This might introduce subtle interactions with custom Expirations.
    // However, this is correct, and people should unwrap the connection
    // if they want to access the raw isValid and its behaviour.
    return !isClosed && con.isValid(timeout);
  }
  
  
  
  
  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException(
          "The Class object given to unwrap cannot be null.");
    }
    
    Object obj = unwrapObject(iface);
    if (obj != NOT_WRAPPED) {
      return (T) obj;
    }
    if (con.isWrapperFor(iface)) {
      return con.unwrap(iface);
    }
    throw new SQLException("Found no wrapped implementation of " + iface);
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException(
          "The Class object given to isWrapperFor cannot be null.");
    }
    
    boolean canDirectlyUnwrap = unwrapObject(iface) != NOT_WRAPPED;
    return canDirectlyUnwrap || con.isWrapperFor(iface);
  }
  
  private Object unwrapObject(Class<?> type) {
    if (type.isAssignableFrom(con.getClass())) {
      return con;
    }
    return NOT_WRAPPED;
  }
  


  

  @Override
  public PreparedStatement prepareStatement(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int autoGeneratedKeys)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int[] columnIndexes)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, String[] columnNames)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }
  
  @Override
  public PreparedStatement prepareStatement(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public CallableStatement prepareCall(String sql, int resultSetType,
      int resultSetConcurrency, int resultSetHoldability) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }
  
  
  

  @Override
  public Statement createStatement() throws SQLException {
    assertNotClosed();
    return con.createStatement();
  }

  @Override
  public Statement createStatement(int resultSetType, int resultSetConcurrency)
      throws SQLException {
    assertNotClosed();
    return con.createStatement(resultSetType, resultSetConcurrency);
  }

  @Override
  public Statement createStatement(
      int resultSetType,
      int resultSetConcurrency,
      int resultSetHoldability) throws SQLException {
    assertNotClosed();
    return con.createStatement(
        resultSetType, resultSetConcurrency, resultSetHoldability);
  }

  @Override
  public String nativeSQL(String sql) throws SQLException {
    assertNotClosed();
    return con.nativeSQL(sql);
  }

  @Override
  public void setAutoCommit(boolean autoCommit) throws SQLException {
    assertNotClosed();
    con.setAutoCommit(autoCommit);
  }

  @Override
  public boolean getAutoCommit() throws SQLException {
    assertNotClosed();
    return con.getAutoCommit();
  }

  @Override
  public void commit() throws SQLException {
    assertNotClosed();
    con.commit();
  }

  @Override
  public void rollback() throws SQLException {
    assertNotClosed();
    con.rollback();
  }

  @Override
  public void rollback(Savepoint savepoint) throws SQLException {
    assertNotClosed();
    con.rollback(savepoint);
  }

  @Override
  public Savepoint setSavepoint() throws SQLException {
    assertNotClosed();
    return con.setSavepoint();
  }

  @Override
  public Savepoint setSavepoint(String name) throws SQLException {
    assertNotClosed();
    return con.setSavepoint(name);
  }

  @Override
  public void releaseSavepoint(Savepoint savepoint) throws SQLException {
    assertNotClosed();
    con.releaseSavepoint(savepoint);
  }

  @Override
  public DatabaseMetaData getMetaData() throws SQLException {
    assertNotClosed();
    return con.getMetaData();
  }

  @Override
  public void setReadOnly(boolean readOnly) throws SQLException {
    assertNotClosed();
    con.setReadOnly(readOnly);
  }

  @Override
  public boolean isReadOnly() throws SQLException {
    assertNotClosed();
    return con.isReadOnly();
  }

  @Override
  public void setCatalog(String catalog) throws SQLException {
    assertNotClosed();
    con.setCatalog(catalog);
  }

  @Override
  public String getCatalog() throws SQLException {
    assertNotClosed();
    return con.getCatalog();
  }

  @Override
  public void setTransactionIsolation(int level) throws SQLException {
    assertNotClosed();
    con.setTransactionIsolation(level);
  }

  @Override
  public int getTransactionIsolation() throws SQLException {
    assertNotClosed();
    return con.getTransactionIsolation();
  }

  @Override
  public SQLWarning getWarnings() throws SQLException {
    assertNotClosed();
    return con.getWarnings();
  }

  @Override
  public void clearWarnings() throws SQLException {
    assertNotClosed();
    con.clearWarnings();
  }

  @Override
  public Map<String, Class<?>> getTypeMap() throws SQLException {
    assertNotClosed();
    return con.getTypeMap();
  }

  @Override
  public void setTypeMap(Map<String, Class<?>> map) throws SQLException {
    assertNotClosed();
    con.setTypeMap(map);
  }

  @Override
  public void setHoldability(int holdability) throws SQLException {
    assertNotClosed();
    con.setHoldability(holdability);
  }

  @Override
  public int getHoldability() throws SQLException {
    assertNotClosed();
    return con.getHoldability();
  }

  @Override
  public Clob createClob() throws SQLException {
    assertNotClosed();
    return con.createClob();
  }

  @Override
  public Blob createBlob() throws SQLException {
    assertNotClosed();
    return con.createBlob();
  }

  @Override
  public NClob createNClob() throws SQLException {
    assertNotClosed();
    return con.createNClob();
  }

  @Override
  public SQLXML createSQLXML() throws SQLException {
    assertNotClosed();
    return con.createSQLXML();
  }

  @Override
  public void setClientInfo(String name, String value)
      throws SQLClientInfoException {
    if (isClosed) {
      Map<String, ClientInfoStatus> failures =
          new HashMap<String, ClientInfoStatus>();
      failures.put(name, ClientInfoStatus.REASON_UNKNOWN);
      throw newClientInfoException(failures);
    }
    con.setClientInfo(name, value);
  }

  @Override
  public void setClientInfo(Properties properties)
      throws SQLClientInfoException {
    if (isClosed) {
      Map<String, ClientInfoStatus> failures =
          new HashMap<String, ClientInfoStatus>();
      for (Map.Entry<Object, Object> entry : properties.entrySet()) {
        String key = entry.getKey().toString();
        failures.put(key, ClientInfoStatus.REASON_UNKNOWN);
      }
      throw newClientInfoException(failures);
    }
    con.setClientInfo(properties);
  }

  private SQLClientInfoException newClientInfoException(
      Map<String, ClientInfoStatus> failures) {
    SQLException cause = newConnectionClosedException();
    return new SQLClientInfoException(CLOSED_MESSAGE, failures, cause);
  }

  @Override
  public String getClientInfo(String name) throws SQLException {
    assertNotClosed();
    return con.getClientInfo(name);
  }

  @Override
  public Properties getClientInfo() throws SQLException {
    assertNotClosed();
    return con.getClientInfo();
  }

  @Override
  public Array createArrayOf(String typeName, Object[] elements)
      throws SQLException {
    assertNotClosed();
    return con.createArrayOf(typeName, elements);
  }

  @Override
  public Struct createStruct(String typeName, Object[] attributes)
      throws SQLException {
    assertNotClosed();
    return con.createStruct(typeName, attributes);
  }
}
