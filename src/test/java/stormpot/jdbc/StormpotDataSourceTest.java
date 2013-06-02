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

import stormpot.LifecycledPool;
import stormpot.LifecycledResizablePool;
import stormpot.Pool;
import stormpot.ResizablePool;

public class StormpotDataSourceTest {
  private static final PrintWriter LOG_WRITER =
      new PrintWriter(new StringWriter());
  
  class Fixture {
    DataSource delegate;

    public Fixture() {
      delegate = mock(DataSource.class);
    }

    public DataSource pool() {
      return new StormpotDataSource(delegate);
    }
  }
  
  public Fixture fixture() {
    return new Fixture();
  }
  
  // -----------------------------------------------------------------------
  
  // constructors:
  @Test(expected = IllegalArgumentException.class) public void
  dataSourceCannotBeNull() {
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
    verify(fixture.delegate).setLogWriter(LOG_WRITER);
  }
  
  @Test public void
  mustNotRememberLogWriterIfDelegateThrows() throws SQLException {
    Fixture fixture = fixture();
    doThrow(new SQLException()).when(fixture.delegate).setLogWriter(LOG_WRITER);
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
    verify(fixture.delegate).setLoginTimeout(10);
  }
  
  @Test public void
  mustNotSetLoginTimeoutIfDelegateThrows() throws SQLException {
    Fixture fixture = fixture();
    doThrow(new SQLException()).when(fixture.delegate).setLoginTimeout(10);
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
    fixture.delegate = new BlockingDataSource();
    DataSource ds = fixture.pool();
    ds.setLoginTimeout(1);
    long start = System.nanoTime();
    try {
      ds.getConnection();
      fail("Should have thrown an exception about timeout!");
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
    assertThat(ds.unwrap(DataSource.class), sameInstance(fixture.delegate));
  }
  
  @Test public void
  canUnwrapDataSourceSpecificClass() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate = new BlockingDataSource();
    DataSource ds = fixture.pool();
    assertTrue(ds.isWrapperFor(BlockingDataSource.class));
  }
  
  @Test public void
  mustUnwrapDataSourceSpecificClass() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate = new BlockingDataSource();
    DataSource ds = fixture.pool();
    assertThat(
        ds.unwrap(BlockingDataSource.class), sameInstance(fixture.delegate));
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
    when(fixture.delegate.isWrapperFor(String.class)).thenReturn(true);
    DataSource ds = fixture.pool();
    assertTrue(ds.isWrapperFor(String.class));
  }
  
  @Test public void
  unwrapMustDelegateForUnknownTypes() throws SQLException {
    String obj = "a string";
    Fixture fixture = fixture();
    when(fixture.delegate.isWrapperFor(String.class)).thenReturn(true);
    when(fixture.delegate.unwrap(String.class)).thenReturn(obj);
    DataSource ds = fixture.pool();
    assertThat(ds.unwrap(String.class), is(obj));
  }

  // -----------------------------------------------------------------------
  
  // javax.sql.DataSource:
  @Test public void
  getConnectionMustClaimFromPool() throws SQLException {
    Connection con = mock(Connection.class);
    Fixture fixture = fixture();
    when(fixture.delegate.getConnection()).thenReturn(con);
    DataSource ds = fixture.pool();
    Connection proxy = ds.getConnection();
    assertThat(proxy.unwrap(Connection.class), sameInstance(con));
  }
  
  @Test(expected = SQLTimeoutException.class) public void
  mustThrowIfClaimTimesOut() throws SQLException {
    Fixture fixture = fixture();
    fixture.delegate = new BlockingDataSource();
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
    when(fixture.delegate.getConnection()).thenThrow(exception);
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
}
