package springer.jdbc;

import java.sql.Connection;

public interface IJdbcWrite {

    // generate key

    public IKeyHolder genkey(Connection conn, String sql, Object[] params);

    // update

    public int update(Connection conn, String sql, Object[] params);

    public int[] batchUpdate(Connection conn, String sql, Object[][] paramsArray);

}
