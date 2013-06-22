package stormpot.jdbc.stubs;

/**
 * A test DataSource where the getConnection methods always block.
 */
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.concurrent.locks.LockSupport;
import java.util.logging.Logger;

import javax.sql.DataSource;

public class BlockingDataSourceStub implements DataSource {
  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return null;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    return 0;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    return false;
  }

  @Override
  public Connection getConnection() throws SQLException {
    LockSupport.park();
    return null;
  }

  @Override
  public Connection getConnection(String username, String password)
      throws SQLException {
    LockSupport.park();
    return null;
  }

  // JDBC 4.1 / JDK 1.7:
  
  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    return null;
  }
}
