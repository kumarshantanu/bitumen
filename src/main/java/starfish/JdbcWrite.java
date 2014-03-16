package starfish;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface JdbcWrite {

    // generate key

    public List<Map<String, Object>> genkey(Connection conn, String sql, Object[] args);

    // update

    public int update(Connection conn, String sql, Object[] args);

    public int[] batchUpdate(Connection conn, String sql, Object[][] argsArray);

}
