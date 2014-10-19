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

public class SqlParams implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String sql;
    public final Object[] params;

    public SqlParams(String sql) {
        this(sql, new Object[0]);
    }

    public SqlParams(String sql, Object[] params) {
        this(sql, params, new DefaultJdbcRead(), new DefaultJdbcWrite());
    }

    public SqlParams(String sql, Object[] args, IJdbcRead reader, IJdbcWrite writer) {
        this.sql = sql;
        this.params = args;
        this.reader = reader;
        this.writer = writer;
    }

    public transient final IJdbcRead reader;
    public transient final IJdbcWrite writer;

    // ===== fluent interface methods =====

    public SqlParams usingParams(Object[] params) {
        return new SqlParams(sql, params, reader, writer);
    }

    public SqlParams usingReader(IJdbcRead reader) {
        return new SqlParams(sql, params, reader, writer);
    }

    public SqlParams usingReader(IJdbcWrite writer) {
        return new SqlParams(sql, params, reader, writer);
    }

    // ----- Reader methods -----

    public List<Map<String, Object>> queryForList(Connection conn) {
        return reader.queryForList(conn, sql, params);
    }

    public List<Map<String, Object>> queryForList(Connection conn, long limit, boolean throwLimitExceedException) {
        return reader.queryForList(conn, sql, params, limit, throwLimitExceedException);
    }

    public <T> List<T> queryForList(Connection conn, IRowExtractor<T> extractor) {
        return reader.queryForList(conn, sql, params, extractor);
    }

    public <T> List<T> queryForList(Connection conn, IRowExtractor<T> extractor, long limit,
            boolean throwLimitExceedException) {
        return reader.queryForList(conn, sql, params, extractor, limit, throwLimitExceedException);
    }

    public <K, V> Map<K, V> queryForMap(Connection conn, IRowExtractor<K> keyExtractor, IRowExtractor<V> valueExtractor) {
        return reader.queryForMap(conn, sql, params, keyExtractor, valueExtractor);
    }

    public <K, V> Map<K, V> queryForMap(Connection conn, IRowExtractor<K> keyExtractor, IRowExtractor<V> valueExtractor,
            long limit, boolean throwLimitExceedException) {
        return reader.queryForMap(conn, sql, params, keyExtractor, valueExtractor, limit, throwLimitExceedException);
    }

    public <T> T queryCustom(Connection conn, IResultSetExtractor<T> extractor) {
        return reader.queryCustom(conn, sql, params, extractor);
    }

    // ----- Writer methods -----

    public IKeyHolder genkey(Connection conn) {
        return writer.genkey(conn, sql, params);
    }

    public int update(Connection conn) {
        return writer.update(conn, sql, params);
    }

    public int[] batchUpdate(Connection conn, Object[][] argsArray) {
        return writer.batchUpdate(conn, sql, argsArray);
    }

    // ===== Object methods =====

    @Override
    public String toString() {
        return String.format("sql = %s, args = %s", sql, Arrays.toString(params));
    }

    @Override
    public int hashCode() {
        return ("" + sql + '|' + Arrays.toString(params)).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SqlParams)) {
            return false;
        }
        final SqlParams that = (SqlParams) obj;
        return Util.equals(sql, that.sql) && Util.equals(params, that.params);
    }

}
