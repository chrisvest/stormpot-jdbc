package stormpot.jdbc;

import static org.mockito.Mockito.*;

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
    ConnectionProxy proxy = new ConnectionProxy(slot, con);
    proxy.release();
    verify(slot).release(proxy);
  }
  
  @Test public void
  closeIsEquivalentToRelease() throws SQLException {
    ConnectionProxy proxy = new ConnectionProxy(slot, con);
    proxy.close();
    verify(slot).release(proxy);
  }
}
