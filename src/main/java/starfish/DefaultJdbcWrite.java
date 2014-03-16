package starfish;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import starfish.helper.JdbcUtil;
import starfish.helper.Util;

public class DefaultJdbcWrite implements JdbcWrite {

    public List<Map<String, Object>> genkey(Connection conn, String sql, Object[] args) {
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithArgs(conn, sql, args);
        try {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s], args: %s",
                    sql, Arrays.toString(args)), e);
        }
        try {
            final ResultSet rs = pstmt.getGeneratedKeys();
            return DefaultJdbcRead.extractMaps(rs, JdbcRead.NO_LIMIT, JdbcRead.NO_LIMIT_EXCEED_EXCEPTION);
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to extract gnerated keys for SQL statement: [%s], args: %s",
                    sql, Arrays.toString(args)), e);
        }
    }

    public int update(Connection conn, String sql, Object[] args) {
        Util.echo("Update SQL: [%s], args: %s\n", sql, Arrays.toString(args));
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithArgs(conn, sql, args);
        try {
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s], args: %s",
                    sql, Arrays.toString(args)), e);
        } finally {
            JdbcUtil.close(pstmt);
        }
    }

    public int[] batchUpdate(Connection conn, String sql, Object[][] argsArray) {
        Util.echo("Update SQL: [%s], batch-size: %d, args: %s\n",
                sql, argsArray.length, Arrays.toString(JdbcUtil.eachStr(argsArray)));
        final PreparedStatement pstmt = JdbcUtil.prepareStatement(conn, sql);
        for (Object[] args: argsArray) {
            JdbcUtil.prepareArgs(pstmt, args);
            try {
                pstmt.addBatch();
            } catch (SQLException e) {
                JdbcUtil.close(pstmt);
                throw new JdbcException(String.format("Unable to add batch arguments for SQL: [%s], args: %s",
                        sql, Arrays.toString(args)), e);
            }
        }
        try {
            return pstmt.executeBatch();
        } catch (SQLException e) {
            JdbcUtil.close(pstmt);
            throw new JdbcException(String.format("Unable to execute batch for SQL: [%s] (batch size = %d)",
                    sql, argsArray.length), e);
        } finally {
            JdbcUtil.close(pstmt);
        }
    }

}
