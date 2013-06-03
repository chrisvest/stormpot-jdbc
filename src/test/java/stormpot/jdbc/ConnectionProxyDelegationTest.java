package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.SQLException;
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
}
