package stormpot.jdbc;

import static org.hamcrest.Matchers.sameInstance;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import stormpot.Slot;
import stormpot.jdbc.stubs.ConnectionStub;

public class ConnectionProxyWrapperTest {
  private static final AdaptorFactory adaptor =
      AdaptorMetaFactory.getAdaptorFactory();
  Slot slot;
  Jdbc41Connection con;
  
  @Before public void
  setUp() throws SQLException {
    slot = mock(Slot.class);
    con = mock(Jdbc41ConnectionDelegate.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(con.getMetaData()).thenReturn(metaData);
  }

  private ConnectionProxy proxy() throws SQLException {
    return new ConnectionProxy(slot, adaptor.adapt(con));
  }
  
  @Test public void
  canUnwrapConnectionInterface() throws SQLException {
    ConnectionProxy proxy = proxy();
    assertTrue(proxy.isWrapperFor(Connection.class));
  }
  
  @Test public void
  mustUnwrapConnectionInterface() throws SQLException {
    ConnectionProxy proxy = proxy();
    assertThat(proxy.unwrap(Connection.class), sameInstance((Object) con));
  }
  
  @Test public void
  canUnwrapConnectionSpecificClass() throws SQLException {
    con = new ConnectionStub();
    ConnectionProxy proxy = proxy();
    assertTrue(proxy.isWrapperFor(ConnectionStub.class));
  }
  
  @Test public void
  mustUnwrapConnectionSpecificClass() throws SQLException {
    con = new ConnectionStub();
    ConnectionProxy proxy = proxy();
    assertThat(proxy.unwrap(ConnectionStub.class), sameInstance(con));
  }
  
  @Test public void
  canUnwrapIfDelegateCanUnwrap() throws SQLException {
    when(con.isWrapperFor(String.class)).thenReturn(true);
    ConnectionProxy proxy = proxy();
    assertTrue(proxy.isWrapperFor(String.class));
  }
  
  @Test public void
  unwrapMustDelegateForUnknownTypes() throws SQLException {
    String obj = "a string";
    when(con.isWrapperFor(String.class)).thenReturn(true);
    when(con.unwrap(String.class)).thenReturn(obj);
    ConnectionProxy proxy = proxy();
    assertThat(proxy.unwrap(String.class), sameInstance(obj));
  }
  
  @Test public void
  cannotUnwrapTotallyUnknownTypes() throws SQLException {
    ConnectionProxy proxy = proxy();
    assertFalse(proxy.isWrapperFor(String.class));
  }
  
  @Test(expected = SQLException.class) public void
  unwrapMustThrowSQLEceptionForTotallyUnkownTypes() throws SQLException {
    ConnectionProxy proxy = proxy();
    proxy.unwrap(String.class);
  }
  
  @Test(expected = SQLException.class) public void
  isWrapperForMustThrowOnNulls() throws SQLException {
    ConnectionProxy proxy = proxy();
    proxy.isWrapperFor(null);
  }
  
  @Test(expected = SQLException.class) public void
  unwrapMustThrowOnNulls() throws SQLException {
    ConnectionProxy proxy = proxy();
    proxy.unwrap(null);
  }
}
