package springer.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import springer.JdbcException;
import springer.JdbcRead;
import springer.JdbcWrite;
import springer.helper.JdbcUtil;
import springer.helper.Util;

public class DefaultJdbcWrite implements JdbcWrite {

    public GeneratedKeyHolder genkey(Connection conn, String sql, Object[] params) {
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithParams(conn, sql, params, true);
        try {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s], args: %s",
                    sql, Arrays.toString(params)), e);
        }
        try {
            final ResultSet rs = pstmt.getGeneratedKeys();
            return new GeneratedKeyHolder(DefaultJdbcRead.extractMaps(rs, JdbcRead.NO_LIMIT,
                    JdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to extract gnerated keys for SQL statement: [%s], args: %s",
                    sql, Arrays.toString(params)), e);
        }
    }

    public int update(Connection conn, String sql, Object[] params) {
        Util.echo("Update SQL: [%s], args: %s\n", sql, Arrays.toString(params));
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithParams(conn, sql, params);
        try {
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s], args: %s",
                    sql, Arrays.toString(params)), e);
        } finally {
            JdbcUtil.close(pstmt);
        }
    }

    public int[] batchUpdate(Connection conn, String sql, Object[][] paramsArray) {
        Util.echo("Update SQL: [%s], batch-size: %d, args: %s\n",
                sql, paramsArray.length, Arrays.toString(JdbcUtil.eachStr(paramsArray)));
        final PreparedStatement pstmt = JdbcUtil.prepareStatement(conn, sql);
        for (Object[] args: paramsArray) {
            JdbcUtil.prepareParams(pstmt, args);
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
                    sql, paramsArray.length), e);
        } finally {
            JdbcUtil.close(pstmt);
        }
    }

}
