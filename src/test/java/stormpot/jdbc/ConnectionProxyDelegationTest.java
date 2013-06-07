package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.DatabaseMetaData;
import java.sql.NClob;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.SQLNonTransientException;
import java.sql.Struct;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.junit.Before;
import org.junit.Test;

import stormpot.Slot;
import stormpot.jdbc.stubs.ArrayStub;
import stormpot.jdbc.stubs.BlobStub;
import stormpot.jdbc.stubs.ClobStub;
import stormpot.jdbc.stubs.DatabaseMetaDataStub;
import stormpot.jdbc.stubs.NClobStub;
import stormpot.jdbc.stubs.SQLXMLStub;
import stormpot.jdbc.stubs.SavepointStub;
import stormpot.jdbc.stubs.StatementStub;
import stormpot.jdbc.stubs.StructStub;

public class ConnectionProxyDelegationTest {
  Statement statementStub = new StatementStub();
  
  Slot slot;
  Jdbc41ConnectionDelegate con;
  ConnectionProxy proxy;
  
  @Before public void
  setUp() {
    slot = mock(Slot.class);
    con = mock(Jdbc41ConnectionDelegate.class);
    proxy = new ConnectionProxy(slot, con);
  }
  
  @Test public void
  mustDelegateCreateStatement1() throws SQLException {
    when(con.createStatement()).thenReturn(statementStub);
    Statement statement1 = proxy.createStatement();
    assertThat(statement1, sameInstance(statementStub));
  }
  
  @Test public void
  mustDelegateCreateStatement2() throws SQLException {
    when(con.createStatement(1, 2)).thenReturn(statementStub);
    Statement statement2 = proxy.createStatement(1, 2);
    assertThat(statement2, sameInstance(statementStub));
  }
  
  @Test public void
  mustDelegateCreateStatement3() throws SQLException {
    when(con.createStatement(3, 4, 5)).thenReturn(statementStub);
    Statement statement3 = proxy.createStatement(3, 4, 5);
    assertThat(statement3, sameInstance(statementStub));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createStatementMustThrowIfClosed1() throws SQLException {
    proxy.close();
    proxy.createStatement();
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createStatementMustThrowIfClosed2() throws SQLException {
    proxy.close();
    proxy.createStatement(1, 2);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createStatementMustThrowIfClosed3() throws SQLException {
    proxy.close();
    proxy.createStatement(1, 2, 3);
  }
  
  @Test public void
  mustDelegateNativeSQL() throws SQLException {
    when(con.nativeSQL("a")).thenReturn("b");
    assertThat(proxy.nativeSQL("a"), is("b"));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  nativeSQLMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.nativeSQL("a");
  }
  
  @Test public void
  mustDelegateSetAutoCommit() throws SQLException {
    proxy.setAutoCommit(true);
    verify(con).setAutoCommit(true);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setAutoCommitMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setAutoCommit(true);
  }
  
  @Test public void
  mustDelegateGetAutoCommit() throws SQLException {
    when(con.getAutoCommit()).thenReturn(true, false);
    assertTrue(proxy.getAutoCommit());
    assertFalse(proxy.getAutoCommit());
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getAutoCommitMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getAutoCommit();
  }
  
  @Test public void
  mustDelegateCommit() throws SQLException {
    proxy.commit();
    verify(con).commit();
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  commitMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.commit();
  }
  
  @Test public void
  mustDelegateRollback1() throws SQLException {
    proxy.rollback();
    verify(con).rollback();
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  rollbackMustThrowIfClosed1() throws SQLException {
    proxy.close();
    proxy.rollback();
  }
  
  @Test public void
  mustDelegateRollback2() throws SQLException {
    Savepoint savepoint = new SavepointStub();
    proxy.rollback(savepoint);
    verify(con).rollback(savepoint);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  rollbackMustThrowIfClosed2() throws SQLException {
    proxy.close();
    proxy.rollback(new SavepointStub());
  }
  
  @Test public void
  mustDelegateSetSavepoint1() throws SQLException {
    Savepoint savepoint = new SavepointStub();
    when(con.setSavepoint()).thenReturn(savepoint);
    assertThat(proxy.setSavepoint(), sameInstance(savepoint));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setSavepointMustThrowIfClosed1() throws SQLException {
    proxy.close();
    proxy.setSavepoint();
  }
  
  @Test public void
  mustDelegateSetSavepoint2() throws SQLException {
    Savepoint savepoint = new SavepointStub();
    when(con.setSavepoint("name")).thenReturn(savepoint);
    assertThat(proxy.setSavepoint("name"), sameInstance(savepoint));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setSavepointMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setSavepoint("name");
  }
  
  @Test public void
  mustDelegateReleaseSavepoint() throws SQLException {
    Savepoint savepoint = new SavepointStub();
    proxy.releaseSavepoint(savepoint);
    verify(con).releaseSavepoint(savepoint);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  releaseSavepointMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.releaseSavepoint(new SavepointStub());
  }
  
  @Test public void
  mustDelegateGetMetaData() throws SQLException {
    DatabaseMetaData metaData = new DatabaseMetaDataStub();
    when(con.getMetaData()).thenReturn(metaData);
    assertThat(proxy.getMetaData(), sameInstance(metaData));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getMetaDataMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getMetaData();
  }
  
  @Test public void
  mustDelegateSetReadOnly() throws SQLException {
    proxy.setReadOnly(true);
    verify(con).setReadOnly(true);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setReadOnlyMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setReadOnly(false);
  }
  
  @Test public void
  mustDelegateIsReadOnly() throws SQLException {
    when(con.isReadOnly()).thenReturn(true, false);
    assertTrue(proxy.isReadOnly());
    assertFalse(proxy.isReadOnly());
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  isReadOnlyMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.isReadOnly();
  }
  
  @Test public void
  mustDelegateSetCatalog() throws SQLException {
    proxy.setCatalog("cat");
    verify(con).setCatalog("cat");
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setCatalogMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setCatalog("cat");
  }
  
  @Test public void
  mustDelegateGetCatalog() throws SQLException {
    when(con.getCatalog()).thenReturn("cat");
    assertThat(proxy.getCatalog(), is("cat"));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getCatalogMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getCatalog();
  }
  
  @Test public void
  mustDelegateSetTransactionIsolation() throws SQLException {
    proxy.setTransactionIsolation(13);
    verify(con).setTransactionIsolation(13);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setTransactionIsolationMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setTransactionIsolation(13);
  }
  
  @Test public void
  mustDelegateGetTransactionIsolation() throws SQLException {
    when(con.getTransactionIsolation()).thenReturn(13);
    assertThat(proxy.getTransactionIsolation(), is(13));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getTransactionIsolationMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getTransactionIsolation();
  }
  
  @Test public void
  mustDelegateGetWarnings() throws SQLException {
    SQLWarning warning = new SQLWarning();
    when(con.getWarnings()).thenReturn(warning);
    assertThat(proxy.getWarnings(), sameInstance(warning));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getWarningsMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getWarnings();
  }
  
  @Test public void
  mustDelegateClearWarnings() throws SQLException {
    proxy.clearWarnings();
    verify(con).clearWarnings();
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  clearWarningsMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.clearWarnings();
  }
  
  @Test public void
  mustDelegateGetTypeMap() throws SQLException {
    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    when(con.getTypeMap()).thenReturn(typeMap);
    assertThat(proxy.getTypeMap(), sameInstance(typeMap));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getTypeMapMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getTypeMap();
  }
  
  @Test public void
  mustDelegateSetTypeMap() throws SQLException {
    Map<String, Class<?>> typeMap = new HashMap<String, Class<?>>();
    proxy.setTypeMap(typeMap);
    verify(con).setTypeMap(typeMap);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setTypeMapMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setTypeMap(new HashMap<String, Class<?>>());
  }
  
  @Test public void
  mustDelegateSetHoldability() throws SQLException {
    proxy.setHoldability(13);
    verify(con).setHoldability(13);
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setHoldabilityMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setHoldability(13);
  }
  
  @Test public void
  mustDelegateGetHoldability() throws SQLException {
    when(con.getHoldability()).thenReturn(13);
    assertThat(proxy.getHoldability(), is(13));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getHoldabilityMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getHoldability();
  }
  
  @Test public void
  mustDelegateCreateClob() throws SQLException {
    Clob clob = new ClobStub();
    when(con.createClob()).thenReturn(clob);
    assertThat(proxy.createClob(), sameInstance(clob));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createClobMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.createClob();
  }
  
  @Test public void
  mustDelegateCreateBlob() throws SQLException {
    Blob blob = new BlobStub();
    when(con.createBlob()).thenReturn(blob);
    assertThat(proxy.createBlob(), sameInstance(blob));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createBlobMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.createBlob();
  }
  
  @Test public void
  mustDelegateCreateNClob() throws SQLException {
    NClob nclob = new NClobStub();
    when(con.createNClob()).thenReturn(nclob);
    assertThat(proxy.createNClob(), sameInstance(nclob));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createNClobMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.createNClob();
  }
  
  @Test public void
  mustDelegateCreateSQLXML() throws SQLException {
    SQLXML sqlxml = new SQLXMLStub();
    when(con.createSQLXML()).thenReturn(sqlxml);
    assertThat(proxy.createSQLXML(), sameInstance(sqlxml));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createSQLXMLMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.createSQLXML();
  }
  
  @Test public void
  mustDelegateSetClientInfo1() throws SQLClientInfoException {
    proxy.setClientInfo("k", "v");
    verify(con).setClientInfo("k", "v");
  }
  
  @Test(expected = SQLClientInfoException.class) public void
  setClientInfoMustThrowIfClosed1() throws SQLException {
    proxy.close();
    proxy.setClientInfo("k", "v");
  }
  
  @Test public void
  mustDelegateSetClientInfo2() throws SQLClientInfoException {
    Properties properties = new Properties();
    proxy.setClientInfo(properties);
    verify(con).setClientInfo(properties);
  }
  
  @Test(expected = SQLClientInfoException.class) public void
  setClientInfoMustThrowIfClosed2() throws SQLException {
    proxy.close();
    proxy.setClientInfo(new Properties());
  }
  
  @Test public void
  mustDelegateGetClientInfo1() throws SQLException {
    when(con.getClientInfo("k")).thenReturn("v");
    assertThat(proxy.getClientInfo("k"), is("v"));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getClientInfoMustThrowIfClosed1() throws SQLException {
    proxy.close();
    proxy.getClientInfo("k");
  }
  
  @Test public void
  mustDelegateGetClientInfo2() throws SQLException {
    Properties properties = new Properties();
    when(con.getClientInfo()).thenReturn(properties);
    assertThat(proxy.getClientInfo(), sameInstance(properties));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getClientInfoMustThrowIfClosed2() throws SQLException {
    proxy.close();
    proxy.getClientInfo();
  }
  
  @Test public void
  mustDelegateCreateArrayOf() throws SQLException {
    String typeName = "type";
    Object[] elements = new Object[] {"a", "b", "c"};
    Array array = new ArrayStub();
    when(con.createArrayOf(typeName, elements)).thenReturn(array);
    assertThat(proxy.createArrayOf(typeName, elements), sameInstance(array));
    
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createArrayOfMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.createArrayOf("typeName", new Object[] {});
  }
  
  @Test public void
  mustDelegateCreateStruct() throws SQLException {
    String typeName = "type";
    Object[] attributes = new Object[] {"a", "b", "c"};
    Struct struct = new StructStub();
    when(con.createStruct(typeName, attributes)).thenReturn(struct);
    assertThat(proxy.createStruct(typeName, attributes), sameInstance(struct));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  createStructMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.createStruct("type", new Object[] {});
  }
  
  @Test public void
  mustDelegateSetSchema() throws SQLException {
    proxy.setSchema("schema");
    verify(con).setSchema("schema");
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  setSchemaMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.setSchema("schema");
  }
  
  @Test public void
  mustDelegateGetSchema() throws SQLException {
    when(con.getSchema()).thenReturn("schema");
    assertThat(proxy.getSchema(), is("schema"));
  }
  
  @Test(expected = SQLNonTransientException.class) public void
  getSchemaMustThrowIfClosed() throws SQLException {
    proxy.close();
    proxy.getSchema();
  }
}
