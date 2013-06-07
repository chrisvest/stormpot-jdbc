package stormpot.jdbc;

import java.sql.PreparedStatement;

interface Jdbc41PreparedStatementDelegate extends Jdbc41PreparedStatement {
  
  public PreparedStatement _stormpot_delegate();

}
