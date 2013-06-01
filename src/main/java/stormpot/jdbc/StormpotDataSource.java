package stormpot.jdbc;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;

import javax.sql.DataSource;

public class StormpotDataSource implements DataSource {
  private final DataSource delegate;
  
  // volatile ensures safe publication:
  private volatile PrintWriter logWriter;

  public StormpotDataSource(DataSource delegate) {
    if (delegate == null) {
      throw new IllegalArgumentException(
          "The delegate DataSource cannot be null");
    }
    this.delegate = delegate;
  }

  @Override
  public PrintWriter getLogWriter() throws SQLException {
    return logWriter;
  }

  @Override
  public void setLogWriter(PrintWriter out) throws SQLException {
    delegate.setLogWriter(out);
    logWriter = out;
  }

  @Override
  public void setLoginTimeout(int seconds) throws SQLException {
    // TODO Auto-generated method stub
    
  }

  @Override
  public int getLoginTimeout() throws SQLException {
    // TODO Auto-generated method stub
    return 0;
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
