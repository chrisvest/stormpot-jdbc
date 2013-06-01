package stormpot.jdbc;

import java.sql.Connection;

import javax.sql.DataSource;

import stormpot.Allocator;
import stormpot.Slot;

class DataSourceAllocator implements Allocator<ConnectionProxy> {

  private final DataSource delegate;

  public DataSourceAllocator(DataSource delegate) {
    this.delegate = delegate;
  }

  @Override
  public ConnectionProxy allocate(Slot slot) throws Exception {
    Connection con = delegate.getConnection();
    return new ConnectionProxy(slot, con);
  }

  @Override
  public void deallocate(ConnectionProxy proxy) throws Exception {
    proxy.closeDelegateConnection();
  }
}
