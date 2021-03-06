package stormpot.jdbc;

import javax.sql.DataSource;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.SQLTimeoutException;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

import stormpot.Config;
import stormpot.Pool;
import stormpot.PoolException;
import stormpot.Timeout;
import stormpot.bpool.BlazePool;

public class StormpotDataSource implements DataSource {
  // A sentinel object used by the unwrap methods internally, to signal when
  // we a not wrapping any objects of a given type:
  static final Object NOT_WRAPPED = new Object();
  
  private final DataSource delegate;
  private final Pool<ConnectionProxy> pool;
  
  // Fields guarded by the 'this' lock:
  //
  // These fields are protected by synchronised access, because they are
  // replicated to the delegate, and possibly elsewhere. We need to ensure that
  // the access to these fields are atomic. Native monitors is fine for this,
  // because the fields are accessed so rarely.
  private PrintWriter logWriter;
  private int loginTimeoutSeconds;
  // The timeout field is volatile because it is accessed concurrently in the
  // getConnection method.
  private volatile Timeout timeout;
  
  public StormpotDataSource(JdbcConfig jdbcConfig) {
    if (jdbcConfig == null) {
      throw new IllegalArgumentException("JdbcConfig cannot be null");
    }
    synchronized (jdbcConfig) {
      this.delegate = jdbcConfig.getDataSource();
      if (delegate == null) {
        throw new IllegalArgumentException(
            "The delegate DataSource cannot be null");
      }
      jdbcConfig.validate();
      Config<ConnectionProxy> config = jdbcConfig.buildPoolConfig();
      this.pool = new BlazePool<ConnectionProxy>(config);
      this.timeout = new Timeout(30, TimeUnit.SECONDS);
    }
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
    this.timeout = new Timeout(seconds, TimeUnit.SECONDS);
  }

  @Override
  public synchronized int getLoginTimeout() throws SQLException {
    return loginTimeoutSeconds;
  }

  @SuppressWarnings("unchecked")
  @Override
  public <T> T unwrap(Class<T> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException(
          "The Class object given to unwrap cannot be null.");
    }
    
    Object obj = unwrapObject(iface);
    if (obj != NOT_WRAPPED) {
      return (T) obj;
    }
    if (delegate.isWrapperFor(iface)) {
      return delegate.unwrap(iface);
    }
    throw new SQLException("Found no wrapped implementation of " + iface);
  }
  
  private Object unwrapObject(Class<?> type) {
    if (type.isAssignableFrom(delegate.getClass())) {
      return delegate;
    }
    if (type.isAssignableFrom(pool.getClass())) {
      return pool;
    }
    return NOT_WRAPPED;
  }

  @Override
  public boolean isWrapperFor(Class<?> iface) throws SQLException {
    if (iface == null) {
      throw new SQLException(
          "The Class object given to isWrapperFor cannot be null.");
    }
    
    boolean canDirectlyUnwrap = unwrapObject(iface) != NOT_WRAPPED;
    return canDirectlyUnwrap || delegate.isWrapperFor(iface);
  }

  @Override
  public Connection getConnection() throws SQLException {
    try {
      ConnectionProxy con = pool.claim(timeout);
      if (con == null) {
        String reason =
            "Timeout of " + loginTimeoutSeconds + " seconds exceeded, trying " +
        		"to claim a connection from the connection pool for " + delegate;
        throw new SQLTimeoutException(reason);
      }
      con.reopen();
      return con;
    } catch (PoolException e) {
      throw new SQLException("Failed to claim connection from pool.", e);
    } catch (InterruptedException e) {
      throw new SQLException("The current thread is interrupted.", e);
    }
  }

  @Override
  public Connection getConnection(String username, String password)
      throws SQLException {
    throw new SQLFeatureNotSupportedException(
        "The getConnection(username, password) method is not supported by " +
        "the connection pool in this version. You can work around this by " +
        "using the unwrap method to get the delegate DataSource, but then " +
        "it won't be pooled.");
  }

  // JDBC 4.1 / JDK 1.7:

  public Logger getParentLogger() throws SQLFeatureNotSupportedException {
    throw new SQLFeatureNotSupportedException(
        "Stormpot-jdbc does not use java.util.logging, nor does it delegate " +
        "the call to the underlying DataSource because of Java6 " +
        "compatibility. If you need the parent logger, then unwrap the " +
        "underlying DataSource and call getParentLogger on that instead.");
  }
}
