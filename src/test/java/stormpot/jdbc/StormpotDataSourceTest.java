package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.Test;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import stormpot.Config;
import stormpot.LifecycledPool;
import stormpot.LifecycledResizablePool;
import stormpot.Pool;
import stormpot.ResizablePool;
import stormpot.Slot;
import stormpot.jdbc.stubs.BlockingDataSourceStub;
import stormpot.jdbc.stubs.ConnectionStub;
import stormpot.jdbc.stubs.DataSourceStub;

public class StormpotDataSourceTest {
  private static final PrintWriter LOG_WRITER =
      new PrintWriter(new StringWriter());
  
  class Fixture {
    JdbcConfig config;

    public Fixture() {
      DataSource delegate = mock(DataSource.class);
      config = new JdbcConfig();
      config.setDataSource(delegate);
    }

    public DataSource pool() {
      return new StormpotDataSource(config);
    }
    
    public DataSource delegate() {
      return config.getDataSource();
    }
    
    public void delegate(DataSource ds) {
      config.setDataSource(ds);
    }
  }
  
  public Fixture fixture() {
    return new Fixture();
  }
  
  // -----------------------------------------------------------------------
  
  // constructors:
  @Test(expected = IllegalArgumentException.class) public void
  jdbcConfigCannotBeNull() {
    new StormpotDataSource(null);
  }
  
  // -----------------------------------------------------------------------

  // javax.sql.CommonDataSource:
  @Test public void
  logWriterIsInitiallyNull() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.getLogWriter(), nullValue());
  }
  
  @Test public void
  mustRememberConfiguredLogWriter() throws SQLException {
    DataSource ds = fixture().pool();
    ds.setLogWriter(LOG_WRITER);
    assertThat(ds.getLogWriter(), sameInstance(LOG_WRITER));
  }
  
  @Test public void
  mustSetLogWriterOnDelegate() throws SQLException {
    Fixture fixture = fixture();
    DataSource ds = fixture.pool();
    ds.setLogWriter(LOG_WRITER);
    verify(fixture.delegate()).setLogWriter(LOG_WRITER);
  }
  
  @Test public void
  mustNotRememberLogWriterIfDelegateThrows() throws SQLException {
    Fixture fixture = fixture();
    doThrow(new SQLException()).when(
        fixture.delegate()).setLogWriter(LOG_WRITER);
    DataSource ds = fixture.pool();
    try {
      ds.setLogWriter(LOG_WRITER);
      fail("Expected SQLException to bubble out, but it didn't!");
    } catch (SQLException _) {}
    assertThat(ds.getLogWriter(), nullValue());
  }
  
  @Test public void
  loginTimeoutIsInitiallyZero() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.getLoginTimeout(), is(0));
  }
  
  @Test public void
  mustRememberLoginTimeout() throws SQLException {
    DataSource ds = fixture().pool();
    ds.setLoginTimeout(10);
    assertThat(ds.getLoginTimeout(), is(10));
  }
  
  @Test public void
  mustSetLoginTimeoutOnDelegate() throws SQLException {
    Fixture fixture = fixture();
    DataSource ds = fixture.pool();
    ds.setLoginTimeout(10);
    verify(fixture.delegate()).setLoginTimeout(10);
  }
  
  @Test public void
  mustNotSetLoginTimeoutIfDelegateThrows() throws SQLException {
    Fixture fixture = fixture();
    doThrow(new SQLException()).when(
        fixture.delegate()).setLoginTimeout(10);
    DataSource ds = fixture.pool();
    try {
      ds.setLoginTimeout(10);
      fail("Expected SQLException to bubble out, but it didn't!");
    } catch (SQLException _) {}
    assertThat(ds.getLoginTimeout(), is(0));
  }
  
  @Test(timeout = 2000) public void
  mustUseLoginTimeoutAsClaimTimeout() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate(new BlockingDataSourceStub());
    DataSource ds = fixture.pool();
    ds.setLoginTimeout(1);
    long start = System.nanoTime();
    try {
      Connection con = ds.getConnection();
      fail("Should have thrown an exception about timeout! " +
      		"Instead got this after " +
          TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - start) +
      		" milliseconds: " + con);
    } catch (SQLTimeoutException _) {}
    long end = System.nanoTime();
    assertThat(end - start, greaterThanOrEqualTo(TimeUnit.SECONDS.toNanos(1)));
  }

  // -----------------------------------------------------------------------
  
  // java.sql.Wrapper:
  @Test public void
  doesNotWrapUnknownObjects() throws SQLException {
    DataSource ds = fixture().pool();
    assertFalse(ds.isWrapperFor(String.class));
  }
  
  @Test(expected = SQLException.class) public void
  unwrapMustThrowSQLExceptionForUnknownTypes() throws SQLException {
    DataSource ds  = fixture().pool();
    ds.unwrap(String.class);
  }
  
  @Test(expected = SQLException.class) public void
  isWrapperForMustThrowOnNull() throws SQLException {
    DataSource ds = fixture().pool();
    ds.isWrapperFor(null);
  }
  
  @Test(expected = SQLException.class) public void
  unwrapMustThrowOnNulls() throws SQLException {
    DataSource ds = fixture().pool();
    ds.unwrap(null);
  }
  
  @Test public void
  canUnwrapDataSourceInterface() throws SQLException {
    DataSource ds = fixture().pool();
    assertTrue(ds.isWrapperFor(DataSource.class));
  }
  
  @Test public void
  mustUnwrapDataSourceInterface() throws SQLException {
    Fixture fixture = fixture();
    DataSource ds = fixture.pool();
    assertThat(ds.unwrap(DataSource.class),
        sameInstance(fixture.delegate()));
  }
  
  @Test public void
  canUnwrapDataSourceSpecificClass() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate(new BlockingDataSourceStub());
    DataSource ds = fixture.pool();
    assertTrue(ds.isWrapperFor(BlockingDataSourceStub.class));
  }
  
  @Test public void
  mustUnwrapDataSourceSpecificClass() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate(new BlockingDataSourceStub());
    DataSource ds = fixture.pool();
    assertThat(
        ds.unwrap(BlockingDataSourceStub.class),
        sameInstance(fixture.delegate()));
  }
  
  @Test public void
  canUnwrapPool() throws SQLException {
    DataSource ds = fixture().pool();
    assertTrue(ds.isWrapperFor(Pool.class));
  }
  
  @Test public void
  mustUnwrapPool() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.unwrap(Pool.class), not(nullValue()));
  }
  
  @Test public void
  canUnwrapResizablePool() throws SQLException {
    DataSource ds = fixture().pool();
    assertTrue(ds.isWrapperFor(ResizablePool.class));
  }
  
  @Test public void
  mustUnwrapResizablePool() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.unwrap(ResizablePool.class), not(nullValue()));
  }
  
  @Test public void
  canUnwrapLifecycledPool() throws SQLException {
    DataSource ds = fixture().pool();
    assertTrue(ds.isWrapperFor(LifecycledPool.class));
  }
  
  @Test public void
  mustUnwrapLifecycledPool() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.unwrap(LifecycledPool.class), not(nullValue()));
  }
  
  @Test public void
  canUnwrapLifecycledResizablePool() throws SQLException {
    DataSource ds = fixture().pool();
    assertTrue(ds.isWrapperFor(LifecycledResizablePool.class));
  }
  
  @Test public void
  mustUnwrapLifecycledResizablePool() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.unwrap(LifecycledResizablePool.class), not(nullValue()));
  }
  
  @Test public void
  canUnwrapIfDelegateCanUnwrap() throws SQLException {
    Fixture fixture = fixture();
    when(fixture.delegate().isWrapperFor(String.class)).thenReturn(true);
    DataSource ds = fixture.pool();
    assertTrue(ds.isWrapperFor(String.class));
  }
  
  @Test public void
  unwrapMustDelegateForUnknownTypes() throws SQLException {
    String obj = "a string";
    Fixture fixture = fixture();
    when(fixture.delegate().isWrapperFor(String.class)).thenReturn(true);
    when(fixture.delegate().unwrap(String.class)).thenReturn(obj);
    DataSource ds = fixture.pool();
    assertThat(ds.unwrap(String.class), is(obj));
  }

  // -----------------------------------------------------------------------
  
  // javax.sql.DataSource:
  @Test public void
  getConnectionMustClaimFromPool() throws SQLException {
    Connection con = new ConnectionStub();
    Fixture fixture = fixture();
    when(fixture.delegate().getConnection()).thenReturn(con);
    DataSource ds = fixture.pool();
    Connection proxy = ds.getConnection();
    assertThat(proxy.unwrap(Connection.class), sameInstance(con));
  }
  
  @Test(expected = SQLTimeoutException.class) public void
  mustThrowIfClaimTimesOut() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate(new BlockingDataSourceStub());
    DataSource ds = fixture.pool();
    ds.setLoginTimeout(0);
    ds.getConnection();
  }
  
  @Test(expected = SQLException.class) public void
  mustThrowIfClaimIsInterrupted() throws SQLException {
    DataSource ds = fixture().pool();
    Thread.currentThread().interrupt();
    ds.getConnection();
  }
  
  @Test public void
  mustWrapPoolExceptionsFromClaim() throws SQLException {
    Throwable exception = new SQLException("Boom!");
    Fixture fixture = fixture();
    when(fixture.delegate().getConnection()).thenThrow(exception);
    DataSource ds = fixture.pool();
    try {
      ds.getConnection();
      fail("The call to getConnection should have thrown!");
    } catch (SQLException sqle) {
      Throwable poolException = sqle.getCause();
      Throwable allocatorException = poolException.getCause();
      assertThat(allocatorException, sameInstance(exception));
    }
  }
  
  @Test(expected = SQLFeatureNotSupportedException.class) public void
  getConnectionByUsernameAndPasswordIsNotSupported() throws SQLException {
    DataSource ds = fixture().pool();
    ds.getConnection("username", "password");
  }
  
  @Test(expected = SQLFeatureNotSupportedException.class) public void
  getParentLoggerIsNotSupported() throws SQLFeatureNotSupportedException {
    StormpotDataSource ds = (StormpotDataSource) fixture().pool();
    ds.getParentLogger();
  }
  
  @Test public void
  claimedConnectionsMustBeOpen() throws SQLException {
    Fixture fixture = fixture();
    when(fixture.delegate().getConnection()).thenAnswer(newConnectionStub());
    fixture.config.setPoolSize(1);
    DataSource ds = fixture.pool();
    Connection con;
    
    con = ds.getConnection();
    assertFalse(con.isClosed());
    con.close();
    
    con = ds.getConnection();
    assertFalse(con.isClosed());
    con.close();
  }

  private Answer<Connection> newConnectionStub() {
    return new Answer<Connection>() {
      public Connection answer(InvocationOnMock invocation) throws Throwable {
        return new ConnectionStub();
      }
    };
  }
  
  @Test public void
  mustReopenClaimedConnections() throws Exception {
    ConnectionProxy proxy = mock(ConnectionProxy.class);
    final DataSourceAllocator alloc = mock(DataSourceAllocator.class);
    when(alloc.allocate(isA(Slot.class))).thenReturn(proxy);
    
    Fixture fixture = fixture();
    fixture.config = new JdbcConfig() {
      Config<ConnectionProxy> buildPoolConfig() {
        config.setAllocator(alloc);
        return config;
      }
    };
    fixture.config.setDataSource(new DataSourceStub()); // makes it validate
    
    DataSource ds = fixture.pool();
    ds.getConnection().close();
    
    verify(proxy).reopen();
  }
}
