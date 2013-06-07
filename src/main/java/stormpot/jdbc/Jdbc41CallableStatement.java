package stormpot.jdbc;

import java.sql.CallableStatement;
import java.sql.SQLException;

interface Jdbc41CallableStatement extends CallableStatement {

  public void closeOnCompletion() throws SQLException;

  public boolean isCloseOnCompletion() throws SQLException;

  public <T> T getObject(int parameterIndex, Class<T> type) throws SQLException;

  public <T> T getObject(String parameterName, Class<T> type)
      throws SQLException;
}
