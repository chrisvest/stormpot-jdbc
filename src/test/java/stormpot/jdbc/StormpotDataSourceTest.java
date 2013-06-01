package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;

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
  
  // TODO must use login timeout as claim timeout
  
  
  // java.sql.Wrapper:
  // TODO unwrap must throw sql exception for unknown types
  // TODO unwrap must throw on nulls
  // TODO must unwrap delegate data source interface
  // TODO must unwrap delegate data source class
  // TODO must unwrap delegate connection interface
  // TODO must unwrap delegate connection class
  // TODO must unwrap pool
  // TODO must unwrap resizable pool
  // TODO must unwrap lifecycled pool
  // TODO must unwrap lifecycled resizable pool
  
  // javax.sql.DataSource:
  // TODO get connection must claim from pool
  // TODO closing connection must release to pool
  // TODO must throw if claim times out
  // TODO must wrap exceptions from claim
  // TODO get connection by username and password is unsupported
}
