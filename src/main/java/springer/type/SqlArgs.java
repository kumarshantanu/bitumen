package springer.type;

import java.io.Serializable;
import java.sql.Connection;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import springer.JdbcRead;
import springer.JdbcWrite;
import springer.ResultSetExtractor;
import springer.RowExtractor;
import springer.helper.Util;
import springer.impl.DefaultJdbcRead;
import springer.impl.DefaultJdbcWrite;

public class SqlArgs implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String sql;
    public final Object[] args;

    public SqlArgs(String sql) {
        this(sql, new Object[0]);
    }

    public SqlArgs(String sql, Object[] args) {
        this(sql, args, new DefaultJdbcRead(), new DefaultJdbcWrite());
    }

    public SqlArgs(String sql, Object[] args, JdbcRead reader, JdbcWrite writer) {
        this.sql = sql;
        this.args = args;
        this.reader = reader;
        this.writer = writer;
    }

    public transient final JdbcRead reader;
    public transient final JdbcWrite writer;

    // ===== fluent interface methods =====

    public SqlArgs usingReader(JdbcRead reader) {
        return new SqlArgs(sql, args, reader, writer);
    }

    public SqlArgs usingReader(JdbcWrite writer) {
        return new SqlArgs(sql, args, reader, writer);
    }

    // ----- Reader methods -----

    public List<Map<String, Object>> queryForList(Connection conn) {
        return reader.queryForList(conn, sql, args);
    }

    public List<Map<String, Object>> queryForList(Connection conn, long limit, boolean throwLimitExceedException) {
        return reader.queryForList(conn, sql, args, limit, throwLimitExceedException);
    }

    public <T> List<T> queryForList(Connection conn, RowExtractor<T> extractor) {
        return reader.queryForList(conn, sql, args, extractor);
    }

    public <T> List<T> queryForList(Connection conn, RowExtractor<T> extractor, long limit,
            boolean throwLimitExceedException) {
        return reader.queryForList(conn, sql, args, extractor, limit, throwLimitExceedException);
    }

    public <K, V> Map<K, V> queryForMap(Connection conn, RowExtractor<K> keyExtractor, RowExtractor<V> valueExtractor) {
        return reader.queryForMap(conn, sql, args, keyExtractor, valueExtractor);
    }

    public <K, V> Map<K, V> queryForMap(Connection conn, RowExtractor<K> keyExtractor, RowExtractor<V> valueExtractor,
            long limit, boolean throwLimitExceedException) {
        return reader.queryForMap(conn, sql, args, keyExtractor, valueExtractor, limit, throwLimitExceedException);
    }

    public <T> T queryCustom(Connection conn, ResultSetExtractor<T> extractor) {
        return reader.queryCustom(conn, sql, args, extractor);
    }

    // ----- Writer methods -----

    public List<Map<String, Object>> genkey(Connection conn) {
        return writer.genkey(conn, sql, args);
    }

    public int update(Connection conn) {
        return writer.update(conn, sql, args);
    }

    public int[] batchUpdate(Connection conn, Object[][] argsArray) {
        return writer.batchUpdate(conn, sql, argsArray);
    }

    // ===== Object methods =====

    @Override
    public String toString() {
        return String.format("sql = %s, args = %s", sql, Arrays.toString(args));
    }

    @Override
    public int hashCode() {
        return ("" + sql + '|' + Arrays.toString(args)).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof SqlArgs)) {
            return false;
        }
        final SqlArgs that = (SqlArgs) obj;
        return Util.equals(sql, that.sql) && Util.equals(args, that.args);
    }

}
