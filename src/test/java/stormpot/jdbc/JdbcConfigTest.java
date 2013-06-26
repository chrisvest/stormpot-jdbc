package stormpot.jdbc;

import static org.junit.Assert.*;
import static org.hamcrest.Matchers.*;
import static org.mockito.Mockito.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.util.concurrent.TimeUnit;

import javax.sql.DataSource;

import org.junit.Test;

import stormpot.Allocator;
import stormpot.Config;
import stormpot.Expiration;
import stormpot.TimeExpiration;
import stormpot.jdbc.stubs.DataSourceStub;
import stormpot.jdbc.stubs.SlotStub;

public class JdbcConfigTest {
  @Test public void
  mustRememberDataSource() {
    DataSource stub = new DataSourceStub();
    JdbcConfig config = new JdbcConfig();
    config.setDataSource(stub);
    assertThat(config.getDataSource(), sameInstance(stub));
  }
  
  @Test public void
  mustConfigureDataSource() throws Exception {
    DataSource ds = mock(DataSource.class);
    Connection con = mock(Connection.class);
    DatabaseMetaData metaData = mock(DatabaseMetaData.class);
    when(con.getMetaData()).thenReturn(metaData);
    when(ds.getConnection()).thenReturn(con);
    
    JdbcConfig jdbcConfig = new JdbcConfig();
    jdbcConfig.setDataSource(ds);
    Config<ConnectionProxy> config = jdbcConfig.buildPoolConfig();
    Allocator<ConnectionProxy> allocator = config.getAllocator();
    allocator.allocate(new SlotStub());
    verify(ds).getConnection();
  }
  
  @Test public void
  mustRememberPoolSize() {
    JdbcConfig config = new JdbcConfig();
    config.setPoolSize(13);
    assertThat(config.getPoolSize(), is(13));
  }
  
  @Test public void
  mustConfigurePoolSize() {
    JdbcConfig config = new JdbcConfig();
    config.setPoolSize(15);
    assertThat(config.buildPoolConfig().getSize(), is(15));
  }
  
  @Test public void
  mustRememberExpiration() {
    Expiration<? super ConnectionProxy> expiration = new TimeExpiration(1, TimeUnit.SECONDS);
    JdbcConfig config = new JdbcConfig();
    config.setExpiration(expiration);
    assertThat(config.getExpiration(), sameInstance((Object) expiration));
  }
  
  @Test public void
  mustConfigureExpiration() {
    Expiration<? super ConnectionProxy> expiration = new TimeExpiration(1, TimeUnit.SECONDS);
    JdbcConfig config = new JdbcConfig();
    config.setExpiration(expiration);
    assertThat(config.buildPoolConfig().getExpiration(), sameInstance((Object) expiration));
  }
  
  @Test(expected = IllegalArgumentException.class) public void
  validateMustThrowIfNoDataSource() {
    JdbcConfig config = new JdbcConfig();
    config.validate();
  }
  
  @Test public void
  mustBeValidWithDataSourceAndDefaults() {
    JdbcConfig config = new JdbcConfig();
    config.setDataSource(new DataSourceStub());
    config.validate();
  }
}
