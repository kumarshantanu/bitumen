package springer.jdbc.impl;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import springer.jdbc.JdbcException;
import springer.jdbc.IRowExtractor;
import springer.util.Util;

/**
 * JDBC utility methods.
 *
 */
public final class JdbcUtil {

    /**
     * Utility class, hence inaccessible private constructor.
     */
    private JdbcUtil() {
        // do nothing
    }

    /**
     * Get a {@link Connection} from specified {@link DataSource}, execute an activity with the connection and finally
     * clean it up.
     * @param  <V>        return type of the activity
     * @param  dataSource JDBC {@link DataSource} instance
     * @param  activity   activity to perform
     * @return            activity result
     */
    public static <V> V withConnection(final DataSource dataSource, final IConnectionActivity<V> activity) {
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
                Util.swallow(e2);
            }
            throw new JdbcException("Error committing transaction", e);
        } catch (RuntimeException e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                Util.swallow(e2);
            }
            throw e;
        } finally {
            close(conn);
        }
    }

    /**
     * Get a {@link Connection} from specified {@link DataSource}, execute a transaction activity using specified
     * transaction isolation level with the connection and finally commit/rollback transaction and clean up the
     * connection.
     * @param  <V>          return type of the activity
     * @param  dataSource   JDBC {@link DataSource} instance
     * @param  txnIsolation JDBC transaction isolation to apply (as documented in {@link Connection})
     * @param  activity     transaction activity
     * @return              activity result
     */
    public static <V> V withTransaction(final DataSource dataSource,
            final int txnIsolation, final IConnectionActivity<V> activity) {
        return withConnection(dataSource, new IConnectionActivity<V>() {
            public V execute(final Connection conn) {
                requireTransaction(conn, txnIsolation);
                return activity.execute(conn);
            }
        });
    }

    /**
     * Get a {@link Connection} from specified {@link DataSource}, execute a transaction activity using default
     * transaction isolation level with the connection and finally commit/rollback transaction and clean up the
     * connection.
     * @param  <V>        return type of the activity
     * @param  dataSource JDBC {@link DataSource} instance
     * @param  activity   transaction activity
     * @return            activity result
     */
    public static <V> V withTransaction(final DataSource dataSource, final IConnectionActivity<V> activity) {
        return withConnection(dataSource, new IConnectionActivity<V>() {
            public V execute(final Connection conn) {
                requireTransaction(conn);
                return activity.execute(conn);
            }
        });
    }

    /**
     * Get a {@link Connection} from specified {@link DataSource}, execute an activity with the connection and finally
     * clean it up.
     * @param  dataSource JDBC {@link DataSource} instance
     * @param  activity   activity to perform
     */
    public static void withConnectionNoResult(final DataSource dataSource, final IConnectionActivityNoResult activity) {
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
                Util.swallow(e2);
            }
            throw new JdbcException("Error committing transaction", e);
        } catch (RuntimeException e) {
            try {
                if (!conn.getAutoCommit()) {
                    conn.rollback();
                }
            } catch (Exception e2) {
                Util.swallow(e2);
            }
            throw e;
        } finally {
            close(conn);
        }
    }

    /**
     * Get a {@link Connection} from specified {@link DataSource}, execute a transaction activity using specified
     * transaction isolation level with the connection and finally commit/rollback transaction and clean up the
     * connection.
     * @param  dataSource   JDBC {@link DataSource} instance
     * @param  txnIsolation JDBC transaction isolation to apply (as documented in {@link Connection})
     * @param  activity     transaction activity
     */
    public static void withTransactionNoResult(final DataSource dataSource,
            final int txnIsolation, final IConnectionActivityNoResult activity) {
        withConnectionNoResult(dataSource, new IConnectionActivityNoResult() {
            public void execute(final Connection conn) {
                requireTransaction(conn, txnIsolation);
                activity.execute(conn);
            }
        });
    }

    /**
     * Get a {@link Connection} from specified {@link DataSource}, execute a transaction activity using default
     * transaction isolation level with the connection and finally commit/rollback transaction and clean up the
     * connection.
     * @param  dataSource JDBC {@link DataSource} instance
     * @param  activity   transaction activity
     */
    public static void withTransactionNoResult(final DataSource dataSource, final IConnectionActivityNoResult activity) {
        withConnectionNoResult(dataSource, new IConnectionActivityNoResult() {
            public void execute(final Connection conn) {
                requireTransaction(conn);
                activity.execute(conn);
            }
        });
    }

    /**
     * This is a <tt>toString()</tt> equivalent for an array of arrays.
     * @param  arrayOfArrays array of arrays
     * @return               String representation suitable for printing
     */
    public static String[] eachStr(final Object[][] arrayOfArrays) {
        final String[] result = new String[arrayOfArrays.length];
        for (int i = 0; i < arrayOfArrays.length; i++) {
            result[i] = Arrays.toString(arrayOfArrays[i]);
        }
        return result;
    }

    /**
     * Create a row extractor that extracts just a column from every row in a {@link ResultSet}.
     * @param  <T>         type of the column value
     * @param  columnClass type of the column value
     * @param  columnIndex column index in {@link ResultSet} (1 based)
     * @return             row extractor
     */
    public static <T> IRowExtractor<T> makeColumnExtractor(final Class<T> columnClass, final int columnIndex) {
        return new IRowExtractor<T>() {
            public T extract(final ResultSet rs) throws SQLException {
                return columnClass.cast(getValue(rs, columnIndex));
            }
        };
    }

    /**
     * Get column value from current row in a {@link ResultSet}.
     * @param  rs          {@link ResultSet instance}
     * @param  columnIndex column index in {@link ResultSet} (1 based)
     * @return             column value
     * @throws SQLException when {@link ResultSet} related operation throws an exception
     */
    public static Object getValue(final ResultSet rs, final int columnIndex) throws SQLException {
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
                if ("java.sql.Timestamp".equals(metaDataClassName)
                        || "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
                    return rs.getTimestamp(columnIndex);
                } else {
                    return rs.getDate(columnIndex);
                }
            }
            if (data instanceof java.sql.Date
                    && "java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(columnIndex))) {
                return rs.getTimestamp(columnIndex);
            }
        }
        return data;
    }

    /**
     * Obtain {@link Connection} from a specified {@link DataSource} instance.
     * @param  dataSource {@link DataSource} instance
     * @return            {@link Connection} object
     */
    public static Connection getConnection(final DataSource dataSource) {
        Connection conn = null;
        try {
            conn = dataSource.getConnection();
        } catch (SQLException e) {
            throw new JdbcException("Unable to obtain connection from DataSource", e);
        }
        return conn;
    }

    /**
     * Set a {@link Connection} into transaction state without changing the isolation level.
     * @param  conn {@link Connection} object
     */
    public static void requireTransaction(final Connection conn) {
        try {
            if (conn.getAutoCommit()) {
                conn.setAutoCommit(false);
            }
        } catch (SQLException e) {
            throw new JdbcException("Unable to configure transaction", e);
        }
    }

    /**
     * Set a {@link Connection} into transaction state with specified isolation level.
     * @param  conn           {@link Connection} object
     * @param  isolationLevel the transaction isolation level (see {@link Connection} for possible values)
     */
    public static void requireTransaction(final Connection conn, final int isolationLevel) {
        requireTransaction(conn);
        try {
            conn.setTransactionIsolation(isolationLevel);
        } catch (SQLException e) {
            throw new JdbcException("Unable to configure transaction isolation level", e);
        }
    }

    /**
     * Create and return a {@link PreparedStatement} from specified {@link Connection} object, SQL statement and
     * its parameters.
     * @param  conn {@link Connection} object
     * @param  sql  SQL statement
     * @param  args SQL statement parameters
     * @return      {@link PreparedStatement} instance
     */
    public static PreparedStatement prepareStatementWithParams(final Connection conn, final String sql,
            final Object[] args) {
        return prepareStatementWithParams(conn, sql, args, false);
    }

    /**
     * Create and return a {@link PreparedStatement} from specified {@link Connection} object, SQL statement and
     * its parameters.
     * @param  conn                {@link Connection} object
     * @param  sql                 SQL statement
     * @param  args                SQL statement parameters
     * @param  returnGeneratedkeys whether statement should return generated keys
     * @return                     {@link PreparedStatement} instance
     */
    public static PreparedStatement prepareStatementWithParams(final Connection conn, final String sql,
            final Object[] args, final boolean returnGeneratedkeys) {
        PreparedStatement pstmt = prepareStatement(conn, sql, returnGeneratedkeys);
        prepareParams(pstmt, args);
        return pstmt;
    }

    /**
     * Simply create a {@link PreparedStatement} from {@link Connection} object and SQL statement.
     * @param  conn {@link Connection} object
     * @param  sql  SQL statement
     * @return      {@link PreparedStatement} instance
     */
    public static PreparedStatement prepareStatement(final Connection conn, final String sql) {
        return prepareStatement(conn, sql, false);
    }

    /**
     * Simply create a {@link PreparedStatement} from {@link Connection} object and SQL statement.
     * @param  conn                {@link Connection} object
     * @param  sql                 SQL statement
     * @param  returnGeneratedkeys whether statement should return generated keys
     * @return                     {@link PreparedStatement} instance
     */
    public static PreparedStatement prepareStatement(final Connection conn, final String sql,
            final boolean returnGeneratedkeys) {
        try {
            if (returnGeneratedkeys) {
                return conn.prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
            } else {
                return conn.prepareStatement(sql);
            }
        } catch (SQLException e) {
            throw new JdbcException(String.format("Unable to prepare statement for SQL: [%s]", sql), e);
        }
    }

    /**
     * Given a {@link PreparedStatement}, set the specified parameters.
     * @param  pstmt  {@link PreparedStatement} instance
     * @param  params statement parameters
     */
    public static void prepareParams(final PreparedStatement pstmt, final Object[] params) {
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

    /**
     * Close a {@link ResultSet}.
     * @param  rs {@link ResultSet} instance
     */
    public static void close(final ResultSet rs) {
        if (rs == null) {
            return;
        }
        try {
            rs.close();
        } catch (SQLException e) {
            Util.swallow(e);
        }
    }

    /**
     * Close a {@link Statement}.
     * @param  stmt {@link Statement} instance
     */
    public static void close(final Statement stmt) {
        if (stmt == null) {
            return;
        }
        try {
            stmt.close();
        } catch (SQLException e) {
            Util.swallow(e);
        }
    }

    /**
     * Close a {@link Connection}.
     * @param  conn {@link Connection} instance
     */
    public static void close(final Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            Util.swallow(e);
        }
    }

    /**
     * Return SQL statement parameter placeholder string for specified number of parameters.
     * @param  count number of parameters
     * @return       placeholder string
     */
    public static String argPlaceholders(final int count) {
        return Util.repeat("?", count, ", ");
    }

    /**
     * Parse a 'format' string, identify variables and replace them with specified values, and do few more things
     * useful for constructing and returning a {@link SqlParams} instance. '\' is the escape character; '\\' represents
     * a single '\' in the 'format' string. Variable names are prefixed with a 'marker' character, and follow Java
     * variable naming rules.
     * @param  marker         character that is prefixed to every variable name in the 'format' string
     * @param  format         the 'format' string
     * @param  values         values to replace variables with
     * @param  throwOnMissing whether throw exception on finding missing variable names (useful for partial rendering)
     * @param  addToVals      whether variable values to be added to {@link SqlParams} 'params'
     * @param  addVals        what values to add to {@link SqlParams} (different from <tt>values</tt>)
     * @return                {@link SqlParams} instance
     */
    public static SqlParams embedReplace(final char marker, final String format, final Map<String, String> values,
            final boolean throwOnMissing, final boolean addToVals, final Map<String, Object> addVals) {
        final int len = format.length();
        final StringBuilder sb = new StringBuilder(len);
        final List<Object> vals = new ArrayList<Object>();
        boolean escaped = false;
        for (int i = 0; i < len; i++) {
            final char c = format.charAt(i);
            if (c == '\\') {
                if (escaped) {
                    escaped = false;
                    sb.append(c);
                    break;
                }
                if (len - i == 1) {
                    throw new IllegalStateException("Dangling escape character found at the end of string: " + format);
                }
                escaped = true;
            } else if (c == marker) {
                if (escaped) {
                    escaped = false;
                    sb.append(c);
                    break;
                }
                if (len - i == 1) {
                    throw new IllegalStateException(
                            "Dangling marker " + marker + " found at the end of string: " + format);
                }
                final char first = format.charAt(++i);
                if (!Character.isJavaIdentifierStart(first)) {
                    throw new IllegalStateException("Illegal identifier name start '" + first + "' in: " + format);
                }
                final StringBuilder name = new StringBuilder();
                name.append(first);
                i++; // hop to next char
                while (i < len) {
                    final char x = format.charAt(i);
                    if (!Character.isJavaIdentifierPart(x)) {
                        break;
                    }
                    name.append(x);
                    i++; // hop to next char
                }
                final String nameStr = name.toString();
                if (!values.containsKey(nameStr)) {
                    if (throwOnMissing) {
                        throw new IllegalArgumentException("No such key '" + nameStr + "' in: " + values.toString());
                    } else {
                        sb.append(marker).append(nameStr);
                    }
                } else {
                    sb.append(values.get(nameStr));
                    if (addToVals) {
                        vals.add(addVals.get(nameStr));
                    }
                }
                if (i < len) {
                    i--;  // push back index if not end-of-string, so that current char is picked in next pass
                }
            } else {
                escaped = false;
                sb.append(c);
            }
        }
        return new SqlParams(sb.toString(), vals.toArray());
    }

    /**
     * Replace named parameters (i.e. variables with marker character ':') with placeholder '?' and add their values to
     * {@link SqlParams} 'params'. Finally return a valid {@link SqlParams} instance.
     * @param  format the SQL statement with embedded named parameters, e.g. "SELECT * FROM emp WHERE id = :id"
     * @param  values values of named parameters, e.g. {"id" => 10}
     * @return        {@link SqlParams} instance from derived SQL statement and parameter values
     */
    public static SqlParams namedParamReplace(final String format, final Map<String, Object> values) {
        final Map<String, String> subsVals = Util.zipmap(new ArrayList<String>(values.keySet()),
                Util.repeat("?", values.size()));
        return embedReplace(':', format, subsVals, true, true, values);
    }

}
