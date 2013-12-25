package stormpot.jdbc;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.sql.*;
import java.util.*;

import org.junit.Before;
import org.junit.Test;
import stormpot.Slot;

public class ConnectionProxyTest {
  private static final AdaptorFactory adaptor =
      AdaptorMetaFactory.getAdaptorFactory();
  Slot slot;
  Jdbc41Connection con;
  DatabaseMetaData metaData;
  
  @Before public void
  setUp() throws SQLException {
    slot = mock(Slot.class);
    con = mock(Jdbc41ConnectionDelegate.class);
    metaData = mock(DatabaseMetaData.class);
    when(con.getMetaData()).thenReturn(metaData);
  }

  private ConnectionProxy proxy() throws SQLException {
    return new ConnectionProxy(slot, adaptor.adapt(con));
  }
  
  @Test(expected = IllegalArgumentException.class) public void
  slotCannotBeNull() throws SQLException {
    new ConnectionProxy(null, adaptor.adapt(con));
  }
  
  @Test(expected = IllegalArgumentException.class) public void
  connectionCannotBeNull() throws SQLException {
    new ConnectionProxy(slot, null);
  }
  
  @Test public void
  releaseMustDelegateToSlot() throws SQLException {
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
  
  @Test public void
  reopenMustSetAutoCommitToTrue() throws SQLException {
    proxy().reopen();
    verify(con).setAutoCommit(true);
  }
  
  @Test public void
  reopenMustClearWarnings() throws SQLException {
    proxy().reopen();
    verify(con).clearWarnings();
  }
  
  @Test public void
  reopenMustResetHoldability() throws SQLException {
    when(metaData.getResultSetHoldability()).thenReturn(13);
    proxy().reopen();
    verify(con).setHoldability(13);
  }
  
  private static class Athlete {}
  
  @Test public void
  reopenMustResetTheTypeMap() throws SQLException {
    Map<String, Class<?>> baseTypeMap = new HashMap<String, Class<?>>();
    baseTypeMap.put("Schema.ATHLETES", Athlete.class);
    
    Map<String, Class<?>> originalTypeMap = new HashMap<String, Class<?>>();
    originalTypeMap.putAll(baseTypeMap);
    
    when(con.getTypeMap()).thenReturn(originalTypeMap);
    
    ConnectionProxy proxy = proxy();
    proxy.getTypeMap().clear();
    proxy.reopen();
    
    verify(con).setTypeMap(baseTypeMap);
  }
  
  @SuppressWarnings("unchecked")
  @Test public void
  reopenMustNotResetTypeMapIfTheyAreNotSupported() throws SQLException {
    when(con.getTypeMap()).thenThrow(new SQLFeatureNotSupportedException());
    
    ConnectionProxy proxy = proxy();
    try {
      proxy.getTypeMap();
    } catch (Exception _) {}
    proxy.reopen();
    
    verify(con, never()).setTypeMap(anyMap());
  }
  
  @SuppressWarnings("unchecked")
  @Test public void
  mustSetTypeMapToNullIfThatIsTheDefault() throws SQLException {
    when(con.getTypeMap()).thenReturn(null);
    
    ConnectionProxy proxy = proxy();
    proxy.setTypeMap(Collections.EMPTY_MAP);
    proxy.reopen();
    
    verify(con).setTypeMap(null);
  }
  
  @Test public void
  reopenMustResetClientInfo() throws SQLException {
    Properties baseClientInfo = new Properties();
    baseClientInfo.setProperty("a", "b");
    
    Properties originalClientInfo = new Properties();
    originalClientInfo.putAll(baseClientInfo);
    
    when(con.getClientInfo()).thenReturn(originalClientInfo);
    
    ConnectionProxy proxy = proxy();
    proxy.getClientInfo().clear();
    proxy.reopen();
    
    verify(con).setClientInfo(baseClientInfo);
  }
  
  @Test public void
  reopenMustSetClientInfoToNullIfThatIsTheDefault() throws SQLException {
    when(con.getClientInfo()).thenReturn(null);
    
    ConnectionProxy proxy = proxy();
    proxy.setClientInfo(new Properties());
    proxy.reopen();
    
    verify(con).setClientInfo(null);
  }

  // TODO must roll back uncommitted transaction on close
  // TODO must not roll back on close when autocommit is on
}
