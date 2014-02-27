package starfish;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import starfish.helper.JdbcUtil;
import starfish.helper.RowExtractor;
import starfish.helper.Util;
import starfish.type.TableMetadata;
import starfish.type.ValueVersion;

public class GenericOpsRead<K, V> implements IOpsRead<K, V> {

    public final String
    versionFormat          = "SELECT $versionColname FROM $tableName WHERE $keyColname = ?",    // key
    multiVersionFormat     = "SELECT $keyColname, $versionColname FROM $tableName WHERE $keyVersionExpression", // keys-placeholder
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
    public final RowExtractor<V> valExtractor2;
    public final RowExtractor<ValueVersion<V>> valueVersionExtractor12;
    public final RowExtractor<ValueVersion<V>> valueVersionExtractor23;
    public final RowExtractor<Long> versionExtractor1 = JdbcUtil.makeColumnExtractor(Long.class, 1);
    public final RowExtractor<Long> versionExtractor2 = JdbcUtil.makeColumnExtractor(Long.class, 2);
    public final RowExtractor<Long> countExtractor2 = JdbcUtil.makeColumnExtractor(Long.class, 2);

    public GenericOpsRead(final TableMetadata meta, Class<K> keyClass, Class<V> valClass) {
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
        this.valExtractor2 = JdbcUtil.makeColumnExtractor(valClass, 2);
        this.valueVersionExtractor12 = ValueVersion.makeExtractor(valClass, 1, 2);
        this.valueVersionExtractor23 = ValueVersion.makeExtractor(valClass, 2, 3);
    }

    protected String putKeysPlaceholder(String format, int count) {
        final String keysPlaceholder = JdbcUtil.argPlaceholders(count);
        return Util.groovyReplace(batchFetchAllFormat,
                Collections.singletonMap("keysPlaceholder", keysPlaceholder), true);
    }

    protected Map<String, String> keyVersionExpression(int count) {
        return Collections.singletonMap("keyVersionExpression", getFor(count));
    }

    // ---- contains ----

    public Long contains(Connection conn, K key) {
        final List<Long> rows = JdbcUtil.queryVals(conn, versionSql, new Object[] { key }, versionExtractor1);
        return rows.isEmpty()? null: rows.get(0);
    }

    public List<Long> batchContains(Connection conn, List<K> keys) {
        final String sql = putKeysPlaceholder(multiVersionSql, keys.size());
        final Map<K, Long> rows = JdbcUtil.queryMap(conn, sql, keys.toArray(), keyExtractor1, versionExtractor2);
        final List<Long> result = new ArrayList<Long>(keys.size());
        for (K each: keys) {
            result.add(rows.get(each));
        }
        return result;
    }

    // ---- containsVersion ----

    public boolean containsVersion(Connection conn, K key, long version) {
        return JdbcUtil.queryVals(conn, condVersionSql, new Object[] { key, version }, versionExtractor1).get(0) > 0;
    }

    public Map<K, Boolean> batchContainsVersion(Connection conn, Map<K, Long> keyVersions) {
        final String sql = Util.groovyReplace(condMultiVersionSql, keyVersionExpression(keyVersions.size()), true);
        final Object[] args = Util.argsArray(keyVersions);
        final Map<K, Long> keyVersionCount = JdbcUtil.queryMap(conn, sql, args, keyExtractor1, countExtractor2);
        final Map<K, Boolean> result = new LinkedHashMap<K, Boolean>(keyVersionCount.size());
        for (K key: keyVersions.keySet()) {
            Long count = keyVersionCount.get(key);
            result.put(key, count != null && count.longValue() > 0);
        }
        return result;
    }

    // ---- read ----

    public V read(Connection conn, K key) {
        final List<V> rows = JdbcUtil.queryVals(conn, fetchSql, new Object[] { key }, valExtractor2);
        return rows.isEmpty()? null: rows.get(0);
    }

    public Map<K, V> batchRead(Connection conn, List<K> keys) {
        final String sql = putKeysPlaceholder(multiFetchSql, keys.size());
        final Map<K, V> rows = JdbcUtil.queryMap(conn, sql, keys.toArray(), keyExtractor1, valExtractor2);
        return rows;
    }

    // ---- readVersion ----

    public V readVersion(Connection conn, K key, long version) {
        final List<V> rows = JdbcUtil.queryVals(conn, condFetchSql, new Object[] { key, version }, valExtractor2);
        return rows.isEmpty()? null: rows.get(0);
    }

    public Map<K, V> batchReadVersion(Connection conn, Map<K, Long> keyVersions) {
        final String sql = Util.groovyReplace(condMultiFetchSql, keyVersionExpression(keyVersions.size()), true);
        final Object[] args = Util.argsArray(keyVersions);
        return JdbcUtil.queryMap(conn, sql, args, keyExtractor1, valExtractor2);
    }

    // ---- readAll ----

    public ValueVersion<V> readAll(Connection conn, K key) {
        final List<ValueVersion<V>> rows = JdbcUtil.queryVals(
                conn, condFetchSql, new Object[] { key }, valueVersionExtractor12);
        return rows.isEmpty()? null: rows.get(0);
    }

    public Map<K, ValueVersion<V>> batchReadAll(Connection conn, List<K> keys) {
        final String sql = putKeysPlaceholder(batchFetchAllSql, keys.size());
        final Map<K, ValueVersion<V>> rows = JdbcUtil.queryMap(conn, sql, keys.toArray(), keyExtractor1,
                valueVersionExtractor23);
        return rows;
    }

}
