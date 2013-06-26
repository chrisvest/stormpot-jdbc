package stormpot.jdbc;

import javax.sql.DataSource;

import stormpot.Config;
import stormpot.Expiration;

public class JdbcConfig {
  Config<ConnectionProxy> config = new Config<ConnectionProxy>();
  DataSource dataSource;

  public synchronized void setDataSource(DataSource dataSource) {
    this.dataSource = dataSource;
    config.setAllocator(new DataSourceAllocator(dataSource));
  }

  public synchronized DataSource getDataSource() {
    return dataSource;
  }

  synchronized Config<ConnectionProxy> buildPoolConfig() {
    return config;
  }

  public synchronized void setPoolSize(int poolSize) {
    config.setSize(poolSize);
  }

  public synchronized int getPoolSize() {
    return config.getSize();
  }

  public synchronized void setExpiration(Expiration<? super ConnectionProxy> expiration) {
    config.setExpiration(expiration);
  }

  public synchronized Expiration<? super ConnectionProxy> getExpiration() {
    return config.getExpiration();
  }

  public synchronized void validate() {
    config.validate();
  }
}
