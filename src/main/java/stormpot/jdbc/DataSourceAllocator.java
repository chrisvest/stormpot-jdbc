package stormpot.jdbc;

import java.sql.Connection;

import javax.sql.DataSource;

import stormpot.Allocator;
import stormpot.Slot;

class DataSourceAllocator implements Allocator<ConnectionProxy> {
  private static final AdaptorFactory adaptor =
      AdaptorMetaFactory.getAdaptorFactory();

  private final DataSource delegate;

  public DataSourceAllocator(DataSource delegate) {
    this.delegate = delegate;
  }

  @Override
  public ConnectionProxy allocate(Slot slot) throws Exception {
    Connection connection = delegate.getConnection();
    Jdbc41ConnectionDelegate adaptor = adapt(connection);
    return new ConnectionProxy(slot, adaptor);
  }

  private Jdbc41ConnectionDelegate adapt(Connection connection) {
    return adaptor.adapt(connection);
  }

  @Override
  public void deallocate(ConnectionProxy proxy) throws Exception {
    proxy.closeDelegateConnection();
  }
}
