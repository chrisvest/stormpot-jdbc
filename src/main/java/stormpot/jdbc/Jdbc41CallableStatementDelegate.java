package stormpot.jdbc;

import java.sql.CallableStatement;

interface Jdbc41CallableStatementDelegate extends Jdbc41CallableStatement {
  
  public CallableStatement _stormpot_delegate();

}
