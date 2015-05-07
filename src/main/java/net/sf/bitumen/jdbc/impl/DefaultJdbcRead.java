package net.sf.bitumen.jdbc.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import net.sf.bitumen.jdbc.IJdbcRead;
import net.sf.bitumen.jdbc.IResultSetExtractor;
import net.sf.bitumen.jdbc.IRowExtractor;
import net.sf.bitumen.jdbc.JdbcException;
import net.sf.bitumen.util.Util;

/**
 * Default implementation of {@link IJdbcRead}.
 *
 */
public class DefaultJdbcRead implements IJdbcRead {

    @Override
    public final List<Map<String, Object>> queryForList(final Connection conn, final String sql,
            final Iterable<?> params) {
        return queryForList(conn, sql, params, -1, true);
    }

    @Override
    public final List<Map<String, Object>> queryForList(final Connection conn, final String sql,
            final Iterable<?> params, final long limit, final boolean throwLimitExceedException) {
        return queryCustom(conn, sql, params, new IResultSetExtractor<List<Map<String, Object>>>() {
            public List<Map<String, Object>> extract(final ResultSet rs) {
                try {
                    return extractMaps(rs, limit, throwLimitExceedException);
                } catch (SQLException e) {
                    throw new JdbcException(String.format("Unable to get result for SQL: [%s]", sql), e);
                }
            }
        });
    }

    /**
     * Extract a list of rows (where every row is a map of column names to values) from specified {@link ResultSet}.
     * @param  rs                        {@link ResultSet} instance
     * @param  limit                     maximum row count to retrieve
     * @param  throwLimitExceedException whether throw exception when row count exceeds specified limit
     * @return                           list of rows (maps)
     * @throws SQLException              when {@link ResultSet} related operation throws exception
     */
    public static List<Map<String, Object>> extractMaps(final ResultSet rs, final long limit,
            final boolean throwLimitExceedException) throws SQLException {
        final List<Map<String, Object>> result = new ArrayList<Map<String, Object>>(1);
        final ResultSetMetaData rsmd = rs.getMetaData();
        final int colCount = rsmd.getColumnCount();
        final String[] colNames = new String[colCount];
        for (int i = 0; i < colCount; i++) {
            colNames[i] = rsmd.getColumnName(i + 1);
        }
        if (limit >= 0) {
            for (long rowCount = 1; rs.next(); rowCount++) {
                if (rowCount > limit) {
                    if (throwLimitExceedException) {
                        throw new IllegalStateException("Expected max " + limit + " rows but found more");
                    } else {
                        break;
                    }
                }
                final Object[] vals = new Object[colCount];
                for (int i = 0; i < colCount; i++) {
                    vals[i] = JdbcUtil.getValue(rs, i + 1);
                }
                result.add(Util.zipmap(colNames, vals));
            }
        } else {
            while (rs.next()) {
                final Object[] vals = new Object[colCount];
                for (int i = 0; i < colCount; i++) {
                    vals[i] = JdbcUtil.getValue(rs, i + 1);
                }
                result.add(Util.zipmap(colNames, vals));
            }
        }
        return result;
    }

    @Override
    public final <T> List<T> queryForList(final Connection conn, final String sql, final Iterable<?> params,
            final IRowExtractor<T> extractor) {
        return queryForList(conn, sql, params, extractor, -1, true);
    }

    @Override
    public final <T> List<T> queryForList(final Connection conn, final String sql, final Iterable<?> params,
            final IRowExtractor<T> extractor, final long limit, final boolean throwLimitExceedException) {
        return queryCustom(conn, sql, params, new IResultSetExtractor<List<T>>() {
            public List<T> extract(final ResultSet rs) {
                final List<T> result = new ArrayList<T>(1);
                try {
                    if (limit >= 0) {
                        for (long rowCount = 1; rs.next(); rowCount++) {
                            if (rowCount > limit) {
                                if (throwLimitExceedException) {
                                    throw new IllegalStateException("Expected max " + limit + " rows but found more");
                                } else {
                                    break;
                                }
                            }
                            result.add(extractor.extract(rs));
                        }
                    } else {
                        while (rs.next()) {
                            result.add(extractor.extract(rs));
                        }
                    }
                } catch (SQLException e) {
                    throw new JdbcException(String.format("Unable to execute SQL statement: [%s]", sql), e);
                }
                return result;
            }
        });
    }

    @Override
    public final <K, V> Map<K, V> queryForMap(final Connection conn, final String sql, final Iterable<?> params,
            final IRowExtractor<K> keyExtractor, final IRowExtractor<V> valueExtractor) {
        return queryForMap(conn, sql, params, keyExtractor, valueExtractor, -1, true);
    }

    @Override
    public final <K, V> Map<K, V> queryForMap(final Connection conn, final String sql, final Iterable<?> params,
            final IRowExtractor<K> keyExtractor, final IRowExtractor<V> valueExtractor, final long limit,
            final boolean throwLimitExceedException) {
        Util.echo("Query SQL: [%s], args: %s\n", sql, String.valueOf(params));
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithParams(conn, sql, params);
        ResultSet rs = null;
        try {
            rs = pstmt.executeQuery();
            Map<K, V> result = new LinkedHashMap<K, V>();
            if (limit >= 0) {
                for (long rowCount = 1; rs.next(); rowCount++) {
                    if (rowCount > limit) {
                        if (throwLimitExceedException) {
                            throw new IllegalStateException("Expected max " + limit + " rows but found more");
                        } else {
                            break;
                        }
                    }
                    result.put(keyExtractor.extract(rs), valueExtractor.extract(rs));
                }
            } else {
                while (rs.next()) {
                    result.put(keyExtractor.extract(rs), valueExtractor.extract(rs));
                }
            }
            return result;
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s]", sql), e);
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(pstmt);
        }
    }

    @Override
    public final <T> T queryCustom(final Connection conn, final String sql, final Iterable<?> params,
            final IResultSetExtractor<T> extractor) {
        Util.echo("Query SQL: [%s], args: %s\n", sql, String.valueOf(params));
        final PreparedStatement pstmt = JdbcUtil.prepareStatementWithParams(conn, sql, params);
        ResultSet rs = null;
        try {
            rs = pstmt.executeQuery();
            return extractor.extract(rs);
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to execute SQL statement: [%s]", sql), e);
        } finally {
            JdbcUtil.close(rs);
            JdbcUtil.close(pstmt);
        }
    }

}
