package springer.jdbc.impl;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Arrays;

import javax.sql.DataSource;

import springer.jdbc.JdbcException;
import springer.jdbc.IRowExtractor;
import springer.util.Util;

public class JdbcUtil {

    public static <V> V withConnection(DataSource dataSource, IConnectionActivity<V> activity) {
        final Connection conn = getConnection(dataSource);
        try {
            final V result = activity.execute(conn);
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
            return result;
        } catch (SQLException e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                // swallow exception
            }
            throw new JdbcException("Error committing transaction", e);
        } catch (RuntimeException e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                // swallow exception
            }
            throw e;
        } finally {
            close(conn);
        }
    }

    public static <V> V withTransaction(DataSource dataSource,
            final int txnIsolation, final IConnectionActivity<V> activity) {
        return withConnection(dataSource, new IConnectionActivity<V>() {
            public V execute(Connection conn) {
                requireTransaction(conn, txnIsolation);
                return activity.execute(conn);
            }
        });
    }

    public static <V> V withTransaction(DataSource dataSource, final IConnectionActivity<V> activity) {
        return withConnection(dataSource, new IConnectionActivity<V>() {
            public V execute(Connection conn) {
                requireTransaction(conn);
                return activity.execute(conn);
            }
        });
    }

    public static void withConnectionNoResult(DataSource dataSource, IConnectionActivityNoResult activity) {
        final Connection conn = getConnection(dataSource);
        try {
            activity.execute(conn);
            if (!conn.getAutoCommit()) {
                conn.commit();
            }
        } catch (SQLException e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                // swallow exception
            }
            throw new JdbcException("Error committing transaction", e);
        } catch (RuntimeException e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                // swallow exception
            }
            throw e;
        } finally {
            close(conn);
        }
    }

    public static void withTransactionNoResult(DataSource dataSource,
            final int txnIsolation, final IConnectionActivityNoResult activity) {
        withConnectionNoResult(dataSource, new IConnectionActivityNoResult() {
            public void execute(Connection conn) {
                requireTransaction(conn, txnIsolation);
                activity.execute(conn);
            }
        });
    }

    public static void withTransactionNoResult(DataSource dataSource, final IConnectionActivityNoResult activity) {
        withConnectionNoResult(dataSource, new IConnectionActivityNoResult() {
            public void execute(Connection conn) {
                requireTransaction(conn);
                activity.execute(conn);
            }
        });
    }

    public static String[] eachStr(Object[][] arrayOfArrays) {
        final String[] result = new String[arrayOfArrays.length];
        for (int i = 0; i < arrayOfArrays.length; i++) {
            result[i] = Arrays.toString(arrayOfArrays[i]);
        }
        return result;
    }

    public static <T> IRowExtractor<T> makeColumnExtractor(final Class<T> columnClass, final int columnIndex) {
        return new IRowExtractor<T>() {
            public T extract(ResultSet rs) throws SQLException {
                return columnClass.cast(getValue(rs, columnIndex));
            }
        };
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
            throw new JdbcException("Unable to obtain connection from DataSource", e);
        }
        return conn;
    }

    public static void requireTransaction(Connection conn) {
        try {
            if (conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new JdbcException("Unable to configure transaction", e);
        }
    }

    public static void requireTransaction(Connection conn, int isolationLevel) {
        requireTransaction(conn);
        try {
            conn.setTransactionIsolation(isolationLevel);
        } catch (SQLException e) {
            throw new JdbcException("Unable to configure transaction isolation level", e);
        }
    }

    public static PreparedStatement prepareStatementWithParams(Connection conn, String sql, Object[] args) {
        return prepareStatementWithParams(conn, sql, args, false);
    }

    public static PreparedStatement prepareStatementWithParams(Connection conn, String sql, Object[] args,
            boolean returnGeneratedkeys) {
        PreparedStatement pstmt = prepareStatement(conn, sql, returnGeneratedkeys);
        prepareParams(pstmt, args);
        return pstmt;
    }

    public static PreparedStatement prepareStatement(Connection conn, String sql) {
        return prepareStatement(conn, sql, false);
    }

    public static PreparedStatement prepareStatement(Connection conn, String sql, boolean returnGeneratedkeys) {
        try {
            return returnGeneratedkeys?
                    conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS): conn.prepareStatement(sql);
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to prepare statement for SQL: [%s]", sql), e);
        }
    }

    public static void prepareParams(PreparedStatement pstmt, Object[] params) {
        if (params != null) {
            int i = -1;
            try {
                for (i = 1; i <= params.length; i++) {
                    final Object param = params[i - 1];
                    if (param instanceof Integer) {
                        pstmt.setInt(i, (Integer) param);
                    } else if (param instanceof Long) {
                        pstmt.setLong(i, (Long) param);
                    } else if (param instanceof String) {
                        pstmt.setString(i, (String) param);
                    } else if (param instanceof Timestamp) {
                        pstmt.setTimestamp(i, (Timestamp) param);
                    } else {
                        pstmt.setObject(i, param);
                    }
                }
            } catch (SQLException e) {
                close(pstmt);
                throw new JdbcException(String.format("Unable to set parameter for prepared statement: %d %s",
                        i, params[i]), e);
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
