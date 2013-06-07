package stormpot.jdbc;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;

interface AdaptorFactory {

  public Jdbc41ConnectionDelegate adapt(Connection connection);
  
  public Jdbc41CallableStatementDelegate adapt(CallableStatement statement);
  
  public Jdbc41PreparedStatementDelegate adapt(PreparedStatement statement);
}
