package net.sf.bitumen.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;

import net.sf.bitumen.jdbc.IJdbcRead;
import net.sf.bitumen.jdbc.IJdbcWrite;
import net.sf.bitumen.jdbc.JdbcException;
import net.sf.bitumen.util.Util;

/**
 * Default implementation of {@link IJdbcWrite}.
 *
 */
public class DefaultJdbcWrite implements IJdbcWrite {

    @Override
    public final GeneratedKeyHolder genkey(final Connection conn, final String sql, final Object[] params) {
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithParams(conn, sql, params, true);
        try {
            pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s], args: %s",
                    sql, Arrays.toString(params)), e);
        }
        try {
            final ResultSet rs = pstmt.getGeneratedKeys();
            return new GeneratedKeyHolder(DefaultJdbcRead.extractMaps(rs, IJdbcRead.NO_LIMIT,
                    IJdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to extract gnerated keys for SQL statement: [%s], args: %s",
                    sql, Arrays.toString(params)), e);
        }
    }

    @Override
    public final int update(final Connection conn, final String sql, final Object[] params) {
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

    @Override
    public final int[] batchUpdate(final Connection conn, final String sql, final Object[][] paramsBatch) {
        Util.echo("Update SQL: [%s], batch-size: %d, args: %s\n",
                sql, paramsBatch.length, Arrays.toString(JdbcUtil.eachStr(paramsBatch)));
        final PreparedStatement pstmt = JdbcUtil.prepareStatement(conn, sql);
        for (Object[] params: paramsBatch) {
            JdbcUtil.prepareParams(pstmt, params);
            try {
                pstmt.addBatch();
            } catch (SQLException e) {
                JdbcUtil.close(pstmt);
                throw new JdbcException(String.format("Unable to add batch arguments for SQL: [%s], args: %s",
                        sql, Arrays.toString(params)), e);
            }
        }
        try {
            return pstmt.executeBatch();
        } catch (SQLException e) {
            JdbcUtil.close(pstmt);
            throw new JdbcException(String.format("Unable to execute batch for SQL: [%s] (batch size = %d)",
                    sql, paramsBatch.length), e);
        } finally {
            JdbcUtil.close(pstmt);
        }
    }

}
