package starfish.helper;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

public class JdbcUtil {

    public static <V> V withConnection(DataSource dataSource, ConnectionActivity<V> activity) {
        final Connection conn = getConnection(dataSource);
        try {
            V result = activity.execute(conn);
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            return result;
        } catch (Exception e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                // swallow exception
            }
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new IllegalStateException(e);
        } finally {
            close(conn);
        }
    }

    public static void withConnectionNoResult(DataSource dataSource, ConnectionActivityWithoutResult activity) {
        final Connection conn = getConnection(dataSource);
        try {
            activity.execute(conn);
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (Exception e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                // swallow exception
            }
            if (e instanceof RuntimeException) throw (RuntimeException) e;
            throw new IllegalStateException(e);
        } finally {
            close(conn);
        }
    }

    public static int update(Connection conn, String sql, Object[] args) {
        Util.echo("Update SQL: [%s], args: %s\n", sql, Arrays.toString(args));
        final PreparedStatement pstmt = prepareStatementWithArgs(conn, sql, args);
        try {
            return pstmt.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException(
                    String.format("Unable to execute SQL statement: [%s], args: %s", sql, args), e);
        } finally {
            close(pstmt);
        }
    }

    public static int[] batchUpdate(Connection conn, String sql, Object[][] argsArray) {
        Util.echo("Update SQL: [%s], batch-size: %d, args: %s\n", sql, argsArray.length, Arrays.toString(argsArray));
        PreparedStatement pstmt = prepareStatement(conn, sql);
        for (Object[] args: argsArray) {
            prepareArgs(pstmt, args);
            try {
                pstmt.addBatch();
            } catch (SQLException e) {
                close(pstmt);
                throw new IllegalStateException(
                        String.format("Unable to add batch arguments for SQL: [%s], args: %s", sql, args), e);
            }
        }
        try {
            return pstmt.executeBatch();
        } catch (SQLException e) {
            close(pstmt);
            throw new IllegalStateException(
                    String.format("Unable to execute batch for SQL: [%s] (batch size = %d)", sql, argsArray.length), e);
        } finally {
            close(pstmt);
        }
    }

    public static <T> RowExtractor<T> makeColumnExtractor(final Class<T> columnClass, final int columnIndex) {
        return new RowExtractor<T>() {
            public T extract(ResultSet rs) {
                try {
                    return columnClass.cast(getValue(rs, columnIndex));
                } catch (SQLException e) {
                    throw new IllegalStateException("Unable to extract column number " + columnIndex, e);
                }
            }
        };
    }

    public static <T> List<T> queryVals(Connection conn, String sql, Object[] args, RowExtractor<T> extractor) {
        Util.echo("Query SQL: [%s], args: %s\n", sql, Arrays.toString(args));
        final PreparedStatement pstmt = prepareStatementWithArgs(conn, sql, args);
        ResultSet rs = null;
        try {
            rs = pstmt.executeQuery();
            List<T> result = new ArrayList<T>(1);
            while (rs.next()) {
                result.add(extractor.extract(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException(String.format("Unable to execute SQL statement: [%s]", sql), e);
        } finally {
            close(rs);
            close(pstmt);
        }
    }

    public static <K, V> Map<K, V> queryMap(Connection conn, String sql, Object[] args, RowExtractor<K> keyExtractor,
            RowExtractor<V> valueExtractor) {
        Util.echo("Query SQL: [%s], args: %s\n", sql, Arrays.toString(args));
        final PreparedStatement pstmt = prepareStatementWithArgs(conn, sql, args);
        ResultSet rs = null;
        try {
            rs = pstmt.executeQuery();
            Map<K, V> result = new LinkedHashMap<K, V>();
            while (rs.next()) {
                result.put(keyExtractor.extract(rs), valueExtractor.extract(rs));
            }
            return result;
        } catch (SQLException e) {
            throw new IllegalStateException(String.format("Unable to execute SQL statement: [%s]", sql), e);
        } finally {
            close(rs);
            close(pstmt);
        }
    }

    public static Object getValue(ResultSet rs, int columnIndex) throws SQLException {
        final Object data = rs.getObject(columnIndex);
        if (data instanceof Clob) {
            return rs.getString(columnIndex);
        }
        if (data instanceof Blob) {
            return rs.getBytes(columnIndex);
        }
        if (data != null) {
            final String className = data.getClass().getName();
            if (className.startsWith("oracle.sql.TIMESTAMP")) {
                return rs.getTimestamp(columnIndex);
            }
            if (className.startsWith("oracle.sql.DATE")) {
                final String metaDataClassName = rs.getMetaData().getColumnClassName(columnIndex);
                if ("java.sql.Timestamp".equals(metaDataClassName) ||
                        "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
                    return rs.getTimestamp(columnIndex);
                } else {
                    return rs.getDate(columnIndex);
                }
            }
            if (data instanceof java.sql.Date) {
                if ("java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(columnIndex))) {
                    return rs.getTimestamp(columnIndex);
                }
            }
        }
        return data;
    }

    public static Connection getConnection(DataSource dataSource) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to obtain connection from DataSource", e);
        }
        return conn;
    }

    public static void requireTransaction(Connection conn) {
        try {
            if (!conn.getAutoCommit()) {
                conn.setAutoCommit(true);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to configure transaction", e);
        }
    }

    public static void requireTransaction(Connection conn, int isolationLevel) {
        requireTransaction(conn);
        try {
            conn.setTransactionIsolation(isolationLevel);
        } catch (SQLException e) {
            throw new IllegalStateException("Unable to configure transaction isolation level", e);
        }
    }

    public static PreparedStatement prepareStatementWithArgs(Connection conn, String sql, Object[] args) {
        PreparedStatement pstmt = prepareStatement(conn, sql);
        prepareArgs(pstmt, args);
        return pstmt;
    }

    public static PreparedStatement prepareStatement(Connection conn, String sql) {
        try {
            return conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new IllegalStateException(
                    String.format("Unable to prepare statement for SQL: [%s]", sql), e);
        } catch (RuntimeException e) {
            throw e;
        }
    }

    public static void prepareArgs(PreparedStatement pstmt, Object[] args) {
        if (args != null) {
            int i = -1;
            try {
                for (i = 1; i <= args.length; i++) {
                    pstmt.setObject(i, args[i - 1]);
                }
            } catch (SQLException e) {
                close(pstmt);
                throw new IllegalStateException(
                        String.format("Unable to set parameter for prepared statement: %d %s", i, args[i]), e);
            } catch (RuntimeException e) {
                close(pstmt);
                throw e;
            }
        }
    }

    public static void close(ResultSet rs) {
        if (rs == null) return;
        try {
            rs.close();
        } catch (SQLException e) {
            // swallow exception
        }
    }

    public static void close(Statement stmt) {
        if (stmt == null) return;
        try {
            stmt.close();
        } catch (SQLException e) {
            // swallow exception
        }
    }

    public static void close(Connection conn) {
        if (conn == null) return;
        try {
            conn.close();
        } catch (SQLException e) {
            // swallow exception
        }
    }

    public static String argPlaceholders(int count) {
        return Util.repeat("?", count, ", ");
    }

}
