package springer;

import java.sql.Connection;

public interface JdbcWrite {

    // generate key

    public KeyHolder genkey(Connection conn, String sql, Object[] params);

    // update

    public int update(Connection conn, String sql, Object[] params);

    public int[] batchUpdate(Connection conn, String sql, Object[][] paramsArray);

}
