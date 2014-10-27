package springer.jdbc.impl;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import springer.jdbc.IJdbcRead;
import springer.jdbc.IJdbcWrite;
import springer.jdbc.IKeyHolder;
import springer.jdbc.IResultSetExtractor;
import springer.jdbc.IRowExtractor;
import springer.util.Util;

/**
 * A bunch of JDBC read and write operations over encapsulated SQL statement and its parameters.
 *
 */
public class SqlParams implements Serializable {

    /**
     * Class version for {@link Serializable}.
     */
    private static final long serialVersionUID = 1L;

    /**
     * SQL statement.
     */
    private final String sql;

    /**
     * SQL statement parameters.
     */
    private final Object[] params;

    /**
     * Construct instance from only SQL statement.
     * @param  sqlStatement SQL statement
     */
    public SqlParams(final String sqlStatement) {
        this(sqlStatement, new Object[0]);
    }

    /**
     * Construct instance from SQL statement and its parameters.
     * @param  sqlStatement SQL statement
     * @param  sqlParams    SQL statement parameters
     */
    public SqlParams(final String sqlStatement, final Object[] sqlParams) {
        this(sqlStatement, sqlParams, new DefaultJdbcRead(), new DefaultJdbcWrite());
    }

    /**
     * Construct instance from SQL statement, its parameters and JDBC reader and writer instances.
     * @param  sqlStatement SQL statement
     * @param  sqlParams    SQL statement parameters
     * @param  jdbcReader   {@link IJdbcRead} instance
     * @param  jdbcWriter   {@link IJdbcWrite} instance
     */
    public SqlParams(final String sqlStatement, final Object[] sqlParams, final IJdbcRead jdbcReader,
            final IJdbcWrite jdbcWriter) {
        this.sql = sqlStatement;
        this.params = sqlParams.clone();
        this.reader = jdbcReader;
        this.writer = jdbcWriter;
    }

    /**
     * Reader used for JDBC read operations.
     */
    private final transient IJdbcRead reader;

    /**
     * Writer used for JDBC write operations.
     */
    private final transient IJdbcWrite writer;


    // ===== fluent interface methods =====

    /**
     * Fluent style method to set SQL params.
     * @param  sqlParams SQL statement params
     * @return           <tt>this</tt> object
     */
    public final SqlParams usingParams(final Object[] sqlParams) {
        return new SqlParams(sql, sqlParams, reader, writer);
    }

    // ===== getters =====


    /**
     * Getter for SQL statement.
     * @return SQL statement
     */
    public final String getSql() {
        return sql;
    }

    /**
     * Getter for SQL statement params.
     * @return SQL statement params
     */
    public final Object[] getParams() {
        return params.clone();
    }

    /**
     * Fluent style method to set JDBC reader.
     * @param  jdbcReader JDBC reader
     * @return            <tt>this</tt> object
     */
    public final SqlParams usingReader(final IJdbcRead jdbcReader) {
        return new SqlParams(sql, params, jdbcReader, writer);
    }

    /**
     * Fluent style method to set JDBC writer.
     * @param  jdbcWriter JDBC writer
     * @return            <tt>this</tt> object
     */
    public final SqlParams usingReader(final IJdbcWrite jdbcWriter) {
        return new SqlParams(sql, params, reader, jdbcWriter);
    }

    // ----- Reader methods -----

    /**
     * Same as {@link IJdbcRead#queryForList(Connection, String, Object[])}.
     * @param  conn JDBC connection
     * @return      list of rows
     * @see         IJdbcRead#queryForList(Connection, String, Object[])
     */
    public final List<Map<String, Object>> queryForList(final Connection conn) {
        return reader.queryForList(conn, sql, params);
    }

    /**
     * Same as {@link IJdbcRead#queryForList(Connection, String, Object[], long, boolean)}.
     * @param  conn                      JDBC connection
     * @param  limit                     maximum row count
     * @param  throwLimitExceedException whether throw exception when result exceeds limit
     * @return                           list of rows
     * @see                              IJdbcRead#queryForList(Connection, String, Object[], long, boolean)
     */
    public final List<Map<String, Object>> queryForList(final Connection conn, final long limit,
            final boolean throwLimitExceedException) {
        return reader.queryForList(conn, sql, params, limit, throwLimitExceedException);
    }

    /**
     * Same as {@link IJdbcRead#queryForList(Connection, String, Object[], IRowExtractor)}.
     * @param  <T>       extracted row type
     * @param  conn      JDBC connection
     * @param  extractor {@link IRowExtractor} instance
     * @return           list of extracted rows
     * @see              IJdbcRead#queryForList(Connection, String, Object[], IRowExtractor)
     */
    public final <T> List<T> queryForList(final Connection conn, final IRowExtractor<T> extractor) {
        return reader.queryForList(conn, sql, params, extractor);
    }

    /**
     * See as {@link IJdbcRead#queryForList(Connection, String, Object[], IRowExtractor, long, boolean)}.
     * @param  <T>       extracted row type
     * @param  conn      JDBC connection
     * @param  extractor {@link IRowExtractor} instance
     * @param  limit     maximum row count
     * @param  throwLimitExceedException whether throw exception when row count exceeds limit
     * @return           list of extracted rows
     * @see              IJdbcRead#queryForList(Connection, String, Object[], IRowExtractor, long, boolean)
     */
    public final <T> List<T> queryForList(final Connection conn, final IRowExtractor<T> extractor, final long limit,
            final boolean throwLimitExceedException) {
        return reader.queryForList(conn, sql, params, extractor, limit, throwLimitExceedException);
    }

    /**
     * Same as {@link IJdbcRead#queryForMap(Connection, String, Object[], IRowExtractor, IRowExtractor)}.
     * @param  <K>            extracted key type
     * @param  <V>            extracted value type
     * @param  conn           JDBC connection
     * @param  keyExtractor   extractor of keys
     * @param  valueExtractor extractor of values
     * @return                map of extracted keys and extracted values
     * @see                   IJdbcRead#queryForMap(Connection, String, Object[], IRowExtractor, IRowExtractor)
     */
    public final <K, V> Map<K, V> queryForMap(final Connection conn, final IRowExtractor<K> keyExtractor,
            final IRowExtractor<V> valueExtractor) {
        return reader.queryForMap(conn, sql, params, keyExtractor, valueExtractor);
    }

    /**
     * Same as {@link IJdbcRead#queryForMap(Connection, String, Object[], IRowExtractor, IRowExtractor, long, boolean)}.
     * @param  <K>            extracted key type
     * @param  <V>            extracted value type
     * @param  conn           JDBC connection
     * @param  keyExtractor   extractor of keys
     * @param  valueExtractor extractor of values
     * @param  limit          maximum row count
     * @param  throwLimitExceedException whether to throw exception if row count exceeds limit
     * @return                map of extracted keys to extracted values
     * @see            IJdbcRead#queryForMap(Connection, String, Object[], IRowExtractor, IRowExtractor, long, boolean)
     */
    public final <K, V> Map<K, V> queryForMap(final Connection conn, final IRowExtractor<K> keyExtractor,
            final IRowExtractor<V> valueExtractor, final long limit, final boolean throwLimitExceedException) {
        return reader.queryForMap(conn, sql, params, keyExtractor, valueExtractor, limit, throwLimitExceedException);
    }

    /**
     * Same as {@link IJdbcRead#queryCustom(Connection, String, Object[], IResultSetExtractor)}.
     * @param  <T>       extracted result type
     * @param  conn      JDBC connection
     * @param  extractor result extractor
     * @return           whatever that extractor returns
     * @see              IJdbcRead#queryCustom(Connection, String, Object[], IResultSetExtractor)
     */
    public final <T> T queryCustom(final Connection conn, final IResultSetExtractor<T> extractor) {
        return reader.queryCustom(conn, sql, params, extractor);
    }

    // ----- Writer methods -----

    /**
     * Same as {@link IJdbcWrite#genkey(Connection, String, Object[])}.
     * @param  conn JDBC connection
     * @return      generated key holder
     * @see         IJdbcWrite#genkey(Connection, String, Object[])
     */
    public final IKeyHolder genkey(final Connection conn) {
        return writer.genkey(conn, sql, params);
    }

    /**
     * Same as {@link IJdbcWrite#update(Connection, String, Object[])}.
     * @param  conn JDBC connection
     * @return      number of rows affected
     * @see         IJdbcWrite#update(Connection, String, Object[])
     */
    public final int update(final Connection conn) {
        return writer.update(conn, sql, params);
    }

    /**
     * Same as {@link IJdbcWrite#batchUpdate(Connection, String, Object[][])}.
     * @param  conn        JDBC connection
     * @param  paramsBatch batch of SQL params
     * @return             number of rows affected for each param set
     * @see                IJdbcWrite#batchUpdate(Connection, String, Object[][])
     */
    public final int[] batchUpdate(final Connection conn, final Object[][] paramsBatch) {
        return writer.batchUpdate(conn, sql, paramsBatch);
    }

    // ===== Object methods =====

    @Override
    public final String toString() {
        return String.format("sql = %s, args = %s", sql, Arrays.toString(params));
    }

    @Override
    public final int hashCode() {
        final String compositeString = "" + sql + '|' + Arrays.toString(params);
        return compositeString.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null || !(obj instanceof SqlParams)) {
            return false;
        }
        final SqlParams that = (SqlParams) obj;
        return Util.equals(sql, that.sql) && Util.equals(params, that.params);
    }

}
