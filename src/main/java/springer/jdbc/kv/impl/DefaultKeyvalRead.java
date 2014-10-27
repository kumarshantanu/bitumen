package springer.jdbc.kv.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import springer.jdbc.IJdbcRead;
import springer.jdbc.IRowExtractor;
import springer.jdbc.impl.DefaultJdbcRead;
import springer.jdbc.impl.JdbcUtil;
import springer.jdbc.kv.IKeyvalRead;
import springer.jdbc.kv.ValueVersion;
import springer.util.Util;

/**
 * Default implementation of {@link IKeyvalRead}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DefaultKeyvalRead<K, V> implements IKeyvalRead<K, V> {

    /** SQL formats for various purposes. Used to render SQL templates. */
    private static final String
    VERSION_FORMAT            = "SELECT $versionColname FROM $tableName WHERE $keyColname = ?",    // key
    MULTI_VERSION_FORMAT      = "SELECT $keyColname, $versionColname FROM $tableName WHERE $keyColname IN ($keysPlaceholder)", // keys-placeholder
    COND_VERSION_FORMAT       = "SELECT COUNT(*) FROM $tableName WHERE $keyColname = ? AND $versionColname = ?", // key, old-version
    COND_MULTI_VERSION_FORMAT = "SELECT $keyColname, COUNT(*) FROM $tableName WHERE $keyVersionExpression GROUP BY $keyColname", // key, old-version
    FETCH_FORMAT              = "SELECT $valueColname FROM $tableName WHERE $keyColname = ?",  // key
    MULTI_FETCH_FORMAT        = "SELECT $keyColname, $valueColname FROM $tableName WHERE $keyColname IN ($keysPlaceholder)",  // key
    COND_FETCH_FORMAT         = "SELECT $valueColname FROM $tableName WHERE $keyColname = ? AND $versionColname = ?", // key, old-version
    COND_MULTI_FETCH_FORMAT   = "SELECT $keyColname, $valueColname FROM $tableName WHERE $keyVersionExpression", // key, old-version
    FETCH_ALL_FORMAT          = "SELECT $valueColname, $versionColname FROM $tableName WHERE $keyColname = ?",
    BATCH_FETCH_ALL_FORMAT    = "SELECT $keyColname, $valueColname, $versionColname FROM $tableName WHERE $keyColname IN ($keysPlaceholder)";

    /** SQL templates - rendered from formats. */
    private final String
    versionSql, multiVersionSql, condVersionSql, condMultiVersionSql, fetchSql, multiFetchSql,
    condFetchSql, condMultiFetchSql, fetchAllSql, batchFetchAllSql;

    /** Table meta data. */
    private final TableMetadata meta;

    /** JDBC reader. */
    private final IJdbcRead reader;

    /** SQL fragment cache for memoized result. */
    private final ConcurrentMap<Integer, String> keyVersionExpressions = new ConcurrentHashMap<Integer, String>();

    /**
     * Return SQL expression fragment based on number of parameter count - memoize results for speed.
     * @param  count number of expressions
     * @return       SQL expression fragment
     */
    private String keyVersionExpressionFor(final int count) {
        final String expr = keyVersionExpressions.get(count);
        if (expr != null) {
            return expr;
        }
        final String newExpr = Util.repeat("(" + meta.getKeyColname() + "= ? AND " + meta.getVersionColname() + " = ?)",
                count, " OR ");
        keyVersionExpressions.put(count, newExpr);  // idempotent, so skip the check for efficiency
        return newExpr;
    }

    /** Column index 01. */
    private static final int COLUMN_ONE = 1;

    /** Column index 02. */
    private static final int COLUMN_TWO = 2;

    /** Column index 03. */
    private static final int COLUMN_THREE = 3;

    /** Key extractor from column index 1. */
    private final IRowExtractor<K> keyExtractor1;

    /** Value extractor from column index 01. */
    private final IRowExtractor<V> valExtractor1;

    /** Value extractor from column index 02. */
    private final IRowExtractor<V> valExtractor2;

    /** Row extractor that finds value at column index 01 and version at column index 02. */
    private final IRowExtractor<ValueVersion<V>> valueVersionExtractor12;

    /** Row extractor that finds value at column index 02 and version at column index 03. */
    private final IRowExtractor<ValueVersion<V>> valueVersionExtractor23;

    /** Row extractor that finds version at column index 01. */
    private final IRowExtractor<Long> versionExtractor1 = JdbcUtil.makeColumnExtractor(Long.class, COLUMN_ONE);

    /** Row extractor that finds version at column index 02. */
    private final IRowExtractor<Long> versionExtractor2 = JdbcUtil.makeColumnExtractor(Long.class, COLUMN_TWO);

    /** Row extractor that finds count at column index 02. */
    private final IRowExtractor<Long> countExtractor2 = JdbcUtil.makeColumnExtractor(Long.class, COLUMN_TWO);

    /**
     * Construct instance from required parameters and using default JDBC reader instance.
     * @param  tableMeta table meta data
     * @param  keyClass  key class
     * @param  valClass  value class
     */
    public DefaultKeyvalRead(final TableMetadata tableMeta, final Class<K> keyClass, final Class<V> valClass) {
        this(tableMeta, keyClass, valClass, new DefaultJdbcRead());
    }

    /**
     * Construct instance from required parameters.
     * @param tableMeta table meta data
     * @param keyClass  key class
     * @param valClass  value class
     * @param dbReader  JDBC reader
     */
    public DefaultKeyvalRead(final TableMetadata tableMeta, final Class<K> keyClass, final Class<V> valClass,
            final IJdbcRead dbReader) {
        this.meta = tableMeta;
        this.versionSql          = tableMeta.groovyReplace(VERSION_FORMAT);
        this.multiVersionSql     = tableMeta.groovyReplaceKeep(MULTI_VERSION_FORMAT);
        this.condVersionSql      = tableMeta.groovyReplace(COND_VERSION_FORMAT);
        this.condMultiVersionSql = tableMeta.groovyReplaceKeep(COND_MULTI_VERSION_FORMAT);
        this.fetchSql            = tableMeta.groovyReplace(FETCH_FORMAT);
        this.multiFetchSql       = tableMeta.groovyReplaceKeep(MULTI_FETCH_FORMAT);
        this.condFetchSql        = tableMeta.groovyReplace(COND_FETCH_FORMAT);
        this.condMultiFetchSql   = tableMeta.groovyReplaceKeep(COND_MULTI_FETCH_FORMAT);
        this.fetchAllSql         = tableMeta.groovyReplace(FETCH_ALL_FORMAT);
        this.batchFetchAllSql    = tableMeta.groovyReplaceKeep(BATCH_FETCH_ALL_FORMAT);
        this.keyExtractor1 = JdbcUtil.makeColumnExtractor(keyClass, COLUMN_ONE);
        this.valExtractor1 = JdbcUtil.makeColumnExtractor(valClass, COLUMN_ONE);
        this.valExtractor2 = JdbcUtil.makeColumnExtractor(valClass, COLUMN_TWO);
        this.valueVersionExtractor12 = ValueVersion.makeExtractor(valClass, COLUMN_ONE, COLUMN_TWO);
        this.valueVersionExtractor23 = ValueVersion.makeExtractor(valClass, COLUMN_TWO, COLUMN_THREE);
        this.reader = dbReader;
    }

    /**
     * Replace '$keyPlaceholder' variable in given SQL format with specified number of parameter placeholder ('?').
     * @param  format SQL format
     * @param  count  number of placeholder characters
     * @return        SQL param placeholder fragment
     */
    private String putKeysPlaceholder(final String format, final int count) {
        final String keysPlaceholder = JdbcUtil.paramPlaceholders(count);
        return Util.groovyReplace(format, Collections.singletonMap("keysPlaceholder", keysPlaceholder), true);
    }

    /**
     * Return a map containing format value for variable '$keyVersionExpression' as expression to match key and version
     * repeated (delimited by 'OR') specified number of times.
     * @param  count number of expressions
     * @return       SQL expression
     * @see          #keyVersionExpressionFor(int)
     */
    private Map<String, String> keyVersionExpression(final int count) {
        return Collections.singletonMap("keyVersionExpression", keyVersionExpressionFor(count));
    }

    // ---- contains ----

    @Override
    public final Long contains(final Connection conn, final K key) {
        return Util.firstItem(reader.queryForList(conn, versionSql, new Object[] {key}, versionExtractor1, 1,
                IJdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    @Override
    public final List<Long> batchContains(final Connection conn, final List<K> keys) {
        final String sql = putKeysPlaceholder(multiVersionSql, keys.size());
        final Map<K, Long> rows = reader.queryForMap(conn, sql, keys.toArray(), keyExtractor1, versionExtractor2);
        final List<Long> result = new ArrayList<Long>(keys.size());
        for (K each: keys) {
            result.add(rows.get(each));
        }
        return result;
    }

    // ---- containsVersion ----

    @Override
    public final boolean containsVersion(final Connection conn, final K key, final long version) {
        return Util.firstItem(reader.queryForList(
                conn, condVersionSql, new Object[] {key, version}, versionExtractor1, 1,
                IJdbcRead.NO_LIMIT_EXCEED_EXCEPTION)) > 0;
    }

    @Override
    public final Map<K, Boolean> batchContainsVersion(final Connection conn, final Map<K, Long> keyVersions) {
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

    @Override
    public final V read(final Connection conn, final K key) {
        return Util.firstItem(reader.queryForList(conn, fetchSql, new Object[] {key}, valExtractor1, 1,
                IJdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    @Override
    public final Map<K, V> batchRead(final Connection conn, final List<K> keys) {
        final String sql = putKeysPlaceholder(multiFetchSql, keys.size());
        return reader.queryForMap(conn, sql, keys.toArray(), keyExtractor1, valExtractor2);
    }

    // ---- readVersion ----

    @Override
    public final V readForVersion(final Connection conn, final K key, final long version) {
        return Util.firstItem(reader.queryForList(conn, condFetchSql, new Object[] {key, version}, valExtractor1, 1,
                IJdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    @Override
    public final Map<K, V> batchReadForVersion(final Connection conn, final Map<K, Long> keyVersions) {
        final String sql = Util.groovyReplace(condMultiFetchSql, keyVersionExpression(keyVersions.size()), true);
        final Object[] args = Util.argsArray(keyVersions);
        return reader.queryForMap(conn, sql, args, keyExtractor1, valExtractor2);
    }

    // ---- readAll ----

    @Override
    public final ValueVersion<V> readAll(final Connection conn, final K key) {
        return Util.firstItem(reader.queryForList(conn, fetchAllSql, new Object[] {key}, valueVersionExtractor12, 1,
                IJdbcRead.NO_LIMIT_EXCEED_EXCEPTION));
    }

    @Override
    public final Map<K, ValueVersion<V>> batchReadAll(final Connection conn, final List<K> keys) {
        final String sql = putKeysPlaceholder(batchFetchAllSql, keys.size());
        return reader.queryForMap(conn, sql, keys.toArray(), keyExtractor1, valueVersionExtractor23);
    }

}
