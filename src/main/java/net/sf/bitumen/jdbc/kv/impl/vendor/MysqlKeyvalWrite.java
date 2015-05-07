package net.sf.bitumen.jdbc.kv.impl.vendor;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.bitumen.jdbc.IJdbcWrite;
import net.sf.bitumen.jdbc.impl.DefaultJdbcWrite;
import net.sf.bitumen.jdbc.kv.IKeyvalWrite;
import net.sf.bitumen.jdbc.kv.KeyValueVersion;
import net.sf.bitumen.jdbc.kv.impl.DefaultKeyvalWrite;
import net.sf.bitumen.jdbc.kv.impl.TableMetadata;
import net.sf.bitumen.util.NamedParams;
import net.sf.bitumen.util.Util;

/**
 * MySQL specific key-value writer implementation of {@link IKeyvalWrite}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class MysqlKeyvalWrite<K, V> implements IKeyvalWrite<K, V> {

    /** MySQL's UPSERT format. */
    public static final String
    UPSERT_FORMAT = "INSERT INTO $tableName ($keyColname, $valueColname, $versionColname, $createTimestampColname, $updateTimestampColname)"
    + " VALUES (?, ?, ?, $timestampValuePlaceholder, $timestampValuePlaceholder)"
    + " ON DUPLICATE KEY UPDATE $valueColname = ?, $versionColname = ?, $updateTimestampColname = $timestampValuePlaceholder";

    /** Rendered UPSERT SQL template. */
    private final String upsertSql;

    /** JDBC writer. */
    private final IJdbcWrite writer;

    /** {@link IKeyvalWrite} instance for non-MySQL specific (generic) operations. */
    private final DefaultKeyvalWrite<K, V> generic;

    /** Whether to use MySQL's 'NOW()' expression instead of of client's timestamp. */
    private final boolean populateTimestamp;

    /**
     * Construct instance based on minimum arguments.
     * @param meta              table meta data
     * @param useMySQLTimestamp whether to use MySQL's 'NOW()' function or client's timestamp
     */
    public MysqlKeyvalWrite(final TableMetadata meta, final boolean useMySQLTimestamp) {
        this(meta, useMySQLTimestamp, new DefaultJdbcWrite());
    }

    /**
     * Construct instance based on all required arguments.
     * @param meta              table meta data
     * @param useMySQLTimestamp whether to use MySQL's 'NOW()' function or client's timestamp
     * @param dbWriter          JDBC writer for carrying out MySQL specific write operations
     */
    public MysqlKeyvalWrite(final TableMetadata meta, final boolean useMySQLTimestamp, final IJdbcWrite dbWriter) {
        this.writer = dbWriter;
        this.generic = new DefaultKeyvalWrite<K, V>(meta);
        final String template = meta.groovyReplaceKeep(UPSERT_FORMAT);
        this.upsertSql = NamedParams.groovyReplace(template,
                Collections.singletonMap("timestampValuePlaceholder", useMySQLTimestamp ? "NOW()" : "?"), true);
        this.populateTimestamp = !useMySQLTimestamp;
    }

    @Override
    public final long insert(final Connection conn, final K key, final V value) {
        return generic.insert(conn, key, value);
    }

    @Override
    public final long batchInsert(final Connection conn, final Map<K, V> pairs) {
        return generic.batchInsert(conn, pairs);
    }

    // ---- save, regardless of whether they already exist ----

    @Override
    public final long save(final Connection conn, final K key, final V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        writer.update(conn, upsertSql, populateTimestamp
                ? Arrays.asList(key, value, version, now, now, value, version, now)
                : Arrays.asList(key, value, version, value, version));
        return version;
    }

    @Override
    public final long batchSave(final Connection conn, final Map<K, V> pairs) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        final Collection<Iterable<?>> paramsBatch = new ArrayList<>(pairs.size());
        for (Entry<K, V> entry: pairs.entrySet()) {
            final K key = entry.getKey();
            final V value = entry.getValue();
            paramsBatch.add(populateTimestamp
                    ? Arrays.asList(key, value, version, now, now, value, version, now)
                    : Arrays.asList(key, value, version, value, version));
        }
        writer.batchUpdate(conn, upsertSql, paramsBatch);
        return version;
    }

    // ---- swap (requires old version) ----

    @Override
    public final Long swap(final Connection conn, final K key, final V value, final long version) {
        return generic.swap(conn, key, value, version);
    }

    @Override
    public final Long batchSwap(final Connection conn, final Map<K, V> pairs, final long version) {
        return generic.batchSwap(conn, pairs, version);
    }

    @Override
    public final Long batchSwap(final Connection conn, final List<KeyValueVersion<K, V>> triplets) {
        return generic.batchSwap(conn, triplets);
    }

    // ---- touch (update version) ----

    @Override
    public final Long touch(final Connection conn, final K key) {
        return generic.touch(conn, key);
    }

    @Override
    public final Long batchTouch(final Connection conn, final List<K> keys) {
        return generic.batchTouch(conn, keys);
    }

    // ---- delete ----

    @Override
    public final void delete(final Connection conn, final K key) {
        generic.delete(conn, key);
    }

    @Override
    public final void batchDelete(final Connection conn, final List<K> keys) {
        generic.batchDelete(conn, keys);
    }

    // ---- remove (requires old version) ----

    @Override
    public final void remove(final Connection conn, final K key, final long version) {
        generic.remove(conn, key, version);
    }

    @Override
    public final void batchRemove(final Connection conn, final List<K> keys, final long version) {
        generic.batchRemove(conn, keys, version);
    }

    @Override
    public final void batchRemove(final Connection conn, final Map<K, Long> keys) {
        generic.batchRemove(conn, keys);
    }

}
