package stormpot.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class StormpotDataSource implements DataSource {
  private final DataSource delegate;
  
  // Fields guarded by the 'this' lock:
  private PrintWriter logWriter;
  private int loginTimeoutSeconds;
  
  // These fields are protected by synchronised access, because they are
  // replicated to the delegate, and possibly elsewhere. We need to ensure that
  // the access to these fields are atomic. Native monitors is fine for this,
  // because the fields are accessed so rarely.

  public StormpotDataSource(DataSource delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException(
          "The delegate DataSource cannot be null");
    }
    this.delegate = delegate;
  }

  @Override
  public synchronized PrintWriter getLogWriter() throws SQLException {
    return logWriter;
  }

  @Override
  public synchronized void setLogWriter(PrintWriter out) throws SQLException {
    delegate.setLogWriter(out);
    logWriter = out;
  }

  @Override
  public synchronized void setLoginTimeout(int seconds) throws SQLException {
    delegate.setLoginTimeout(seconds);
    this.loginTimeoutSeconds = seconds;
  }

  @Override
  public synchronized int getLoginTimeout() throws SQLException {
    return loginTimeoutSeconds;
  }

  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Connection getConnection() throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Connection getConnection(String username, String password)
      throws SQLException {
    // TODO Auto-generated method stub
    return null;
  }

}
