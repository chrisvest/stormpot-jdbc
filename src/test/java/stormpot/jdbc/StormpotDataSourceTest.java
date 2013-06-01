package stormpot.jdbc;

import static org.mockito.Mockito.*;
import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;

import java.sql.SQLException;

import javax.sql.DataSource;

import org.junit.Test;

public class StormpotDataSourceTest {
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
  
  // constructors:
  @Test(expected = IllegalArgumentException.class) public void
  dataSourceCannotBeNull() {
    new StormpotDataSource(null);
  }

  // javax.sql.CommonDataSource:
  @Test public void
  logWriterIsInitiallyNull() throws SQLException {
    DataSource ds = fixture().pool();
    assertThat(ds.getLogWriter(), nullValue());
  }
  
  // TODO must remember configured log writer
  // TODO must set log writer on delegate
  // TODO must not remember log writer if delegate throws
  // TODO login timeout is initially zero
  // TODO must remember login timeout
  // TODO must set login timeout on delegate
  // TODO must use login timeout as claim timeout
  // TODO must not set login timeout if delegate throws
  
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
