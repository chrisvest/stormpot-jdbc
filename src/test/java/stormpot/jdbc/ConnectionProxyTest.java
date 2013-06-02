package stormpot.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.Before;
import org.junit.Test;

import stormpot.Slot;

public class ConnectionProxyTest {
  Slot slot;
  Connection con;
  
  @Before public void
  setUp() {
    slot = mock(Slot.class);
    con = mock(Connection.class);
  }

  private ConnectionProxy proxy() {
    return new ConnectionProxy(slot, con);
  }
  
  @Test(expected = IllegalArgumentException.class) public void
  slotCannotBeNull() {
    new ConnectionProxy(null, con);
  }
  
  @Test(expected = IllegalArgumentException.class) public void
  connectionCannotBeNull() {
    new ConnectionProxy(slot, null);
  }
  
  @Test public void
  releaseMustDelegateToSlot() {
    ConnectionProxy proxy = proxy();
    proxy.release();
    verify(slot).release(proxy);
  }
  
  @Test public void
  closeIsEquivalentToRelease() throws SQLException {
    ConnectionProxy proxy = proxy();
    proxy.close();
    verify(slot).release(proxy);
  }
  
  @Test public void
  canUnwrapConnectionInterface() throws SQLException {
    ConnectionProxy proxy = proxy();
    assertTrue(proxy.isWrapperFor(Connection.class));
  }
  
  @Test public void
  mustUnwrapConnectionInterface() throws SQLException {
    ConnectionProxy proxy = proxy();
    assertThat(proxy.unwrap(Connection.class), sameInstance(con));
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
