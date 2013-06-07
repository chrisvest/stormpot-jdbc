package stormpot.jdbc.stubs;

import java.sql.SQLException;
import java.sql.Savepoint;

public class SavepointStub implements Savepoint {

  @Override
  public int getSavepointId() throws SQLException {
    return 0;
  }

  @Override
  public String getSavepointName() throws SQLException {
    return null;
  }
}
