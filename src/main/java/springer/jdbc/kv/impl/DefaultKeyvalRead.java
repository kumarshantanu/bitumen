package springer.jdbc.kv.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import springer.jdbc.JdbcRead;
import springer.jdbc.RowExtractor;
import springer.jdbc.helper.JdbcUtil;
import springer.jdbc.impl.DefaultJdbcRead;
import springer.jdbc.kv.KeyvalRead;
import springer.jdbc.type.TableMetadata;
import springer.jdbc.type.ValueVersion;
import springer.util.Util;

public class DefaultKeyvalRead<K, V> implements KeyvalRead<K, V> {

    public static final String
    versionFormat          = "SELECT $versionColname FROM $tableName WHERE $keyColname = ?",    // key
    multiVersionFormat     = "SELECT $keyColname, $versionColname FROM $tableName WHERE $keyColname IN ($keysPlaceholder)", // keys-placeholder
    condVersionFormat      = "SELECT COUNT(*) FROM $tableName WHERE $keyColname = ? AND $versionColname = ?", // key, old-version
    condMultiVersionFormat = "SELECT $keyColname, COUNT(*) FROM $tableName WHERE $keyVersionExpression GROUP BY $keyColname", // key, old-version
    fetchFormat            = "SELECT $valueColname FROM $tableName WHERE $keyColname = ?",  // key
    multiFetchFormat       = "SELECT $keyColname, $valueColname FROM $tableName WHERE $keyColname IN ($keysPlaceholder)",  // key
    condFetchFormat        = "SELECT $valueColname FROM $tableName WHERE $keyColname = ? AND $versionColname = ?", // key, old-version
    condMultiFetchFormat   = "SELECT $keyColname, $valueColname FROM $tableName WHERE $keyVersionExpression", // key, old-version
    fetchAllFormat         = "SELECT $valueColname, $versionColname FROM $tableName WHERE $keyColname = ?",
    batchFetchAllFormat    = "SELECT $keyColname, $valueColname, $versionColname FROM $tableName WHERE $keyColname IN ($keysPlaceholder)";

    public final String
    versionSql, multiVersionSql, condVersionSql, condMultiVersionSql, fetchSql, multiFetchSql,
    condFetchSql, condMultiFetchSql, fetchAllSql, batchFetchAllSql;

    public final TableMetadata meta;
    public final JdbcRead reader;

    private final ConcurrentMap<Integer, String> keyVersionExpressions = new ConcurrentHashMap<Integer, String>();
    private String getFor(int count) {
        final String expr = keyVersionExpressions.get(count);
        if (expr != null) {
            return expr;
        }
        final String newExpr = Util.repeat("(" + meta.keyColname + "= ? AND " + meta.versionColname + " = ?)",
                count, " OR ");
        keyVersionExpressions.put(count, newExpr);  // idempotent, so skip the check for efficiency
        return newExpr;
    }

    public final RowExtractor<K> keyExtractor1;
    public final RowExtractor<V> valExtractor1;
    public final RowExtractor<V> valExtractor2;
    public final RowExtractor<ValueVersion<V>> valueVersionExtractor12;
    public final RowExtractor<ValueVersion<V>> valueVersionExtractor23;
    public final RowExtractor<Long> versionExtractor1 = JdbcUtil.makeColumnExtractor(Long.class, 1);
    public final RowExtractor<Long> versionExtractor2 = JdbcUtil.makeColumnExtractor(Long.class, 2);
    public final RowExtractor<Long> countExtractor2 = JdbcUtil.makeColumnExtractor(Long.class, 2);

    public DefaultKeyvalRead(TableMetadata meta, Class<K> keyClass, Class<V> valClass) {
        this(meta, keyClass, valClass, new DefaultJdbcRead());
    }

    public DefaultKeyvalRead(TableMetadata meta, Class<K> keyClass, Class<V> valClass, JdbcRead reader) {
        this.meta = meta;
        this.versionSql          = meta.groovyReplace(versionFormat);
        this.multiVersionSql     = meta.groovyReplaceKeep(multiVersionFormat);
        this.condVersionSql      = meta.groovyReplace(condVersionFormat);
        this.condMultiVersionSql = meta.groovyReplaceKeep(condMultiVersionFormat);
        this.fetchSql            = meta.groovyReplace(fetchFormat);
        this.multiFetchSql       = meta.groovyReplaceKeep(multiFetchFormat);
        this.condFetchSql        = meta.groovyReplace(condFetchFormat);
        this.condMultiFetchSql   = meta.groovyReplaceKeep(condMultiFetchFormat);
        this.fetchAllSql         = meta.groovyReplace(fetchAllFormat);
        this.batchFetchAllSql    = meta.groovyReplaceKeep(batchFetchAllFormat);
        this.keyExtractor1 = JdbcUtil.makeColumnExtractor(keyClass, 1);
        this.valExtractor1 = JdbcUtil.makeColumnExtractor(valClass, 1);
        this.valExtractor2 = JdbcUtil.makeColumnExtractor(valClass, 2);
        this.valueVersionExtractor12 = ValueVersion.makeExtractor(valClass, 1, 2);
        this.valueVersionExtractor23 = ValueVersion.makeExtractor(valClass, 2, 3);
        this.reader = reader;
    }

    protected String putKeysPlaceholder(String format, int count) {
        final String keysPlaceholder = JdbcUtil.argPlaceholders(count);
        return Util.groovyReplace(format, Collections.singletonMap("keysPlaceholder", keysPlaceholder), true);
    }

    protected Map<String, String> keyVersionExpression(int count) {
        return Collections.singletonMap("keyVersionExpression", getFor(count));
    }

    // ---- contains ----

    public Long contains(Connection conn, K key) {
        return Util.firstItem(reader.queryForList(conn, versionSql, new Object[] { key }, versionExtractor1, 1,
                JdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    public List<Long> batchContains(Connection conn, List<K> keys) {
        final String sql = putKeysPlaceholder(multiVersionSql, keys.size());
        final Map<K, Long> rows = //JdbcUtil.queryMap(conn, sql, keys.toArray(), keyExtractor1, versionExtractor2);
                reader.queryForMap(conn, sql, keys.toArray(), keyExtractor1, versionExtractor2);
        final List<Long> result = new ArrayList<Long>(keys.size());
        for (K each: keys) {
            result.add(rows.get(each));
        }
        return result;
    }

    // ---- containsVersion ----

    public boolean containsVersion(Connection conn, K key, long version) {
        return Util.firstItem(reader.queryForList(
                conn, condVersionSql, new Object[] { key, version }, versionExtractor1, 1,
                JdbcRead.NO_LIMIT_EXCEED_EXCEPTION)) > 0;
    }

    public Map<K, Boolean> batchContainsVersion(Connection conn, Map<K, Long> keyVersions) {
        final String sql = Util.groovyReplace(condMultiVersionSql, keyVersionExpression(keyVersions.size()), true);
        final Object[] args = Util.argsArray(keyVersions);
        final Map<K, Long> keyVersionCount = reader.queryForMap(conn, sql, args, keyExtractor1, countExtractor2);
        final Map<K, Boolean> result = new LinkedHashMap<K, Boolean>(keyVersionCount.size());
        for (K key: keyVersions.keySet()) {
            Long count = keyVersionCount.get(key);
            result.put(key, count != null && count.longValue() > 0);
        }
        return result;
    }

    // ---- read ----

    public V read(Connection conn, K key) {
        return Util.firstItem(reader.queryForList(conn, fetchSql, new Object[] { key }, valExtractor1, 1,
                JdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    public Map<K, V> batchRead(Connection conn, List<K> keys) {
        final String sql = putKeysPlaceholder(multiFetchSql, keys.size());
        return reader.queryForMap(conn, sql, keys.toArray(), keyExtractor1, valExtractor2);
    }

    // ---- readVersion ----

    public V readForVersion(Connection conn, K key, long version) {
        return Util.firstItem(reader.queryForList(conn, condFetchSql, new Object[] { key, version }, valExtractor1, 1,
                JdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    public Map<K, V> batchReadForVersion(Connection conn, Map<K, Long> keyVersions) {
        final String sql = Util.groovyReplace(condMultiFetchSql, keyVersionExpression(keyVersions.size()), true);
        final Object[] args = Util.argsArray(keyVersions);
        return reader.queryForMap(conn, sql, args, keyExtractor1, valExtractor2);
    }

    // ---- readAll ----

    public ValueVersion<V> readAll(Connection conn, K key) {
        return Util.firstItem(reader.queryForList(conn, fetchAllSql, new Object[] { key }, valueVersionExtractor12, 1,
                JdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    public Map<K, ValueVersion<V>> batchReadAll(Connection conn, List<K> keys) {
        final String sql = putKeysPlaceholder(batchFetchAllSql, keys.size());
        return reader.queryForMap(conn, sql, keys.toArray(), keyExtractor1, valueVersionExtractor23);
    }

}
