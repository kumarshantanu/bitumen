package springer.jdbc;

import java.sql.Connection;

public interface IJdbcWrite {

    // generate key

    IKeyHolder genkey(Connection conn, String sql, Object[] params);

    // update

    int update(Connection conn, String sql, Object[] params);

    int[] batchUpdate(Connection conn, String sql, Object[][] paramsArray);

}
