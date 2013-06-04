package stormpot.jdbc;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;

import java.sql.ClientInfoStatus;
import java.sql.Connection;
import java.sql.SQLClientInfoException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;

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
  closingTwiceMustOnlyReleaseOnce() throws SQLException {
    ConnectionProxy proxy = proxy();
    proxy.close();
    proxy.close();
    verify(slot).release(proxy);
  }
  
  @Test public void
  mustBeInStateClosedAfterClose() throws SQLException {
    ConnectionProxy proxy = proxy();
    proxy.close();
    assertTrue(proxy.isClosed());
  }

  @Test public void
  newConnectionsShouldBeValid() throws SQLException {
    when(con.isValid(13)).thenReturn(true);
    assertTrue(proxy().isValid(13));
  }
  
  @Test public void
  connectionsMustBecomeInvalidWhenClosed() throws SQLException {
    when(con.isValid(anyInt())).thenReturn(true);
    ConnectionProxy proxy = proxy();
    proxy.close();
    assertFalse(proxy.isValid(13));
  }
  
  @Test public void
  connectionsShouldBeValidWhenReopened() throws SQLException {
    when(con.isValid(anyInt())).thenReturn(true);
    ConnectionProxy proxy = proxy();
    proxy.close();
    proxy.reopen();
    assertTrue(proxy.isValid(13));
  }
  
  @Test public void
  connectionsMustOtherwiseDelegateToIsValid() throws SQLException {
    when(con.isValid(13)).thenReturn(true);
    when(con.isValid(27)).thenReturn(false);
    ConnectionProxy proxy = proxy();
    assertTrue(proxy.isValid(13));
    assertFalse(proxy.isValid(27));
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
  
  @Test public void
  setClientInfoExceptionMustContainPropertiesThatWereNotSet() throws SQLException {
    // Curiously, this behaviour is only specified for the bulk setClientInfo.
    Properties input = new Properties();
    input.setProperty("a", "b");
    input.setProperty("k", "v");
    Map<String, ClientInfoStatus> output = null;
    ConnectionProxy proxy = proxy();
    try {
      proxy.close();
      proxy.setClientInfo(input);
    } catch (SQLClientInfoException e) {
      output = e.getFailedProperties();
    }
    assertThat(output.keySet(), equalTo((Object) input.keySet()));
    List<ClientInfoStatus> values = new ArrayList<ClientInfoStatus>();
    values.addAll(output.values());
    assertThat(values.size(), is(2));
    assertThat(values.get(0), is(ClientInfoStatus.REASON_UNKNOWN));
    assertThat(values.get(1), is(ClientInfoStatus.REASON_UNKNOWN));
  }
}
