package stormpot.jdbc;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.Executor;

/**
 * Copy the signatures for new methods in JDBC 4.1 / JDK 7, so we can compile
 * and link against them on Java 6.
 *  
 * @author Chris Vest
 */
interface Jdbc41Connection extends Connection {
  
  public void setSchema(String schema) throws SQLException;

  public String getSchema() throws SQLException;

  public void abort(Executor executor) throws SQLException;

  public void setNetworkTimeout(Executor executor, int milliseconds)
      throws SQLException;

  public int getNetworkTimeout() throws SQLException;
}
