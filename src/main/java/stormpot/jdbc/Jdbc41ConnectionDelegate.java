package stormpot.jdbc;

import java.sql.Connection;

interface Jdbc41ConnectionDelegate extends Jdbc41Connection {
  
  public Connection _stormpot_delegate();

}
