package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.hamcrest.Matchers.*;
import static org.junit.Assert.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Before;
import org.junit.Test;

import stormpot.Allocator;
import stormpot.Slot;

public class DataSourceAllocatorTest {
  DataSource delegate;
  Allocator<ConnectionProxy> allocator;
  Connection con;
  
  @Before public void
  setUp() throws SQLException {
    delegate = mock(DataSource.class);
    allocator = new DataSourceAllocator(delegate);
    con = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(con.getMetaData()).thenReturn(metaData);
  }
  
  @Test public void
  allocationMustBuildNewConnections() throws Exception {
    Slot slot = mock(Slot.class);
    when(delegate.getConnection()).thenReturn(con);
    ConnectionProxy proxy = allocator.allocate(slot);
    assertThat(proxy, not(nullValue()));
  }
  
  @Test public void
  deallocationMustCloseConnections() throws Exception {
    Slot slot = mock(Slot.class);
    when(delegate.getConnection()).thenReturn(con);
    ConnectionProxy proxy = allocator.allocate(slot);
    allocator.deallocate(proxy);
    verify(con).close();
  }
  
  @Test(expected = SQLException.class) public void
  allocationMustBubbleExceptionsOut() throws Exception {
    when(delegate.getConnection()).thenThrow(new SQLException());
    Slot slot = mock(Slot.class);
    allocator.allocate(slot);
  }
  
  @Test(expected = SQLException.class) public void
  deallocationMustBubbleExceptionsOut() throws Exception {
    Slot slot = mock(Slot.class);
    when(delegate.getConnection()).thenReturn(con);
    doThrow(new SQLException()).when(con).close();
    ConnectionProxy proxy = allocator.allocate(slot);
    allocator.deallocate(proxy);
  }
}
