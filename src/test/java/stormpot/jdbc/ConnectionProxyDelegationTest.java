package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.SQLNonTransientException;

import org.junit.Before;
import org.junit.Test;

import stormpot.Slot;

public class ConnectionProxyDelegationTest {
  Statement statementStub = new StatementStub();
  
  Slot slot;
  Connection con;
  ConnectionProxy proxy;
  
  @Before public void
  setUp() {
    slot = mock(Slot.class);
    con = mock(Connection.class);
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
}
















