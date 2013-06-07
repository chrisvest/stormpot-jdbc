package stormpot.jdbc;

import java.sql.PreparedStatement;
import java.sql.SQLException;

interface Jdbc41PreparedStatement extends PreparedStatement {
  
  public void closeOnCompletion() throws SQLException;

  public boolean isCloseOnCompletion() throws SQLException;
}
