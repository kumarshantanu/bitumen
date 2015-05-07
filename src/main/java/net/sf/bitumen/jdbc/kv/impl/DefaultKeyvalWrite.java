package net.sf.bitumen.jdbc.kv.impl;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import net.sf.bitumen.jdbc.IJdbcWrite;
import net.sf.bitumen.jdbc.impl.DefaultJdbcWrite;
import net.sf.bitumen.jdbc.kv.IKeyvalWrite;
import net.sf.bitumen.jdbc.kv.KeyValueVersion;
import net.sf.bitumen.util.Util;

/**
 * Default implementation of {@link IKeyvalWrite}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class DefaultKeyvalWrite<K, V> implements IKeyvalWrite<K, V> {

    /** SQL formats for various purposes. Used to render SQL templates. */
    private static final String
    INSERT_FORMAT      = "INSERT INTO $tableName ($keyColname, $valueColname, $versionColname, $createTimestampColname,"
    + " $updateTimestampColname) VALUES (?, ?, ?, ?, ?)", // key, val, version, timestamp[1,2]
    UPDATE_FORMAT      = "UPDATE $tableName SET $valueColname = ?, $versionColname = ?, $updateTimestampColname = ?"
    + " WHERE $keyColname = ?", // val, version, timestamp, key
    SWAP_FORMAT        = "UPDATE $tableName SET $valueColname = ?, $versionColname = ?, $updateTimestampColname = ?"
    + " WHERE $keyColname = ? AND $versionColname = ?", // val, ver, timestamp, key, old-ver
    TOUCH_FORMAT       = "UPDATE $tableName SET $versionColname = ?, $updateTimestampColname = ? WHERE $keyColname = ?", // version, timestamp, key
    DELETE_FORMAT      = "DELETE FROM $tableName WHERE $keyColname = ?",  // key
    COND_DELETE_FORMAT = "DELETE FROM $tableName WHERE $keyColname = ? AND $versionColname = ?"; // key, old-version

    /** SQL templates - rendered from formats. */
    private final String insertSql, updateSql, swapSql, touchSql, deleteSql, condDeleteSql;

    /** JDBC Writer. */
    private final IJdbcWrite writer;

    /**
     * Construct instance from required parameters and using default JDBC reader instance.
     * @param tableMeta table meta data
     */
    public DefaultKeyvalWrite(final TableMetadata tableMeta) {
        this(tableMeta, new DefaultJdbcWrite());
    }

    /**
     * Construct instance from required parameters.
     * @param tableMeta table meta data
     * @param dbWriter  JDBC writer
     */
    public DefaultKeyvalWrite(final TableMetadata tableMeta, final IJdbcWrite dbWriter) {
        this.insertSql     = tableMeta.groovyReplace(INSERT_FORMAT);
        this.updateSql     = tableMeta.groovyReplace(UPDATE_FORMAT);
        this.swapSql       = tableMeta.groovyReplace(SWAP_FORMAT);
        this.touchSql      = tableMeta.groovyReplace(TOUCH_FORMAT);
        this.deleteSql     = tableMeta.groovyReplace(DELETE_FORMAT);
        this.condDeleteSql = tableMeta.groovyReplace(COND_DELETE_FORMAT);
        this.writer = dbWriter;
    }

    // ----- insert -----

    @Override
    public final long insert(final Connection conn, final K key, final V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        writer.update(conn, insertSql, Arrays.asList(key, value, version, now, now));
        return version;
    }

    @Override
    public final long batchInsert(final Connection conn, final Map<K, V> pairs) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        final Collection<Iterable<?>> insertParamBatch = new ArrayList<>(pairs.size());
        for (Entry<K, V> each: pairs.entrySet()) {
            insertParamBatch.add(Arrays.asList(each.getKey(), each.getValue(), version, now, now));
        }
        writer.batchUpdate(conn, insertSql, insertParamBatch);
        return version;
    }

    // ---- save, regardless of whether they already exist ----

    @Override
    public final long save(final Connection conn, final K key, final V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        int rows = writer.update(conn, updateSql, Arrays.asList(value, version, now, key));
        if (rows == 0) {
            writer.update(conn, insertSql, Arrays.asList(key, value, version, now, now));
        }
        return version;
    }

    @Override
    public final long batchSave(final Connection conn, final Map<K, V> pairs) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        final Collection<Iterable<?>> updateParamBatch = new ArrayList<>(pairs.size());
        for (Entry<K, V> each : pairs.entrySet()) {
            updateParamBatch.add(Arrays.asList(each.getValue(), version, now, each.getKey()));
        }
        final int[] rows = writer.batchUpdate(conn, updateSql, updateParamBatch);
        int toInsert = 0;
        for (int i = 0; i < rows.length; i++) {
            if (rows[i] == 0) {
                toInsert++;
            }
        }
        if (toInsert > 0) {
            final Collection<Iterable<?>> insertParamBatch = new ArrayList<>(toInsert);
            int i = 0;
            for (Entry<K, V> each: pairs.entrySet()) {
                if (rows[i++] == 0) {
                    insertParamBatch.add(Arrays.asList(each.getKey(), each.getValue(), version, now, now));
                }
            }
            writer.batchUpdate(conn, insertSql, insertParamBatch);
        }
        return version;
    }

    // ---- swap (requires old version) ----

    @Override
    public final Long swap(final Connection conn, final K key, final V value, final long version) {
        final long tmpVersion = Util.newVersion();
        final long newVersion = version == tmpVersion ? version + 1 : tmpVersion;
        int rowCount = writer.update(conn, swapSql, Arrays.asList(value, newVersion, Util.now(), key, version));
        return rowCount > 0 ? newVersion : null;
    }

    @Override
    public final Long batchSwap(final Connection conn, final Map<K, V> pairs, final long version) {
        final Timestamp now = Util.now();
        final long tmpVersion = Util.newVersion();
        final long newVersion = version == tmpVersion ? version + 1 : tmpVersion;
        final Collection<Iterable<?>> paramBatch = new ArrayList<>(pairs.size());
        for (Entry<K, V> each : pairs.entrySet()) {
            paramBatch.add(Arrays.asList(each.getValue(), newVersion, now, each.getKey(), version));
        }
        final int[] rowCount = writer.batchUpdate(conn, swapSql, paramBatch);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0 ? newVersion : null;
    }

    @Override
    public final Long batchSwap(final Connection conn, final List<KeyValueVersion<K, V>> triplets) {
        final Timestamp now = Util.now();
        final long newVersion = Util.newVersion();
        final Collection<Iterable<?>> paramBatch = new ArrayList<>(triplets.size());
        for (KeyValueVersion<K, V> each : triplets) {
            paramBatch.add(Arrays.asList(each.getValue(), newVersion, now, each.getKey(), each.getVersion()));
        }
        final int[] rowCount = writer.batchUpdate(conn, swapSql, paramBatch);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0 ? newVersion : null;
    }

    // ---- touch (update version) ----

    @Override
    public final Long touch(final Connection conn, final K key) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        final int rowCount = writer.update(conn, touchSql, Arrays.asList(version, now, key));
        return rowCount > 0 ? version : null;
    }

    @Override
    public final Long batchTouch(final Connection conn, final List<K> keys) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        final Collection<Iterable<?>> paramBatch = new ArrayList<>(keys.size());
        for (K each : keys) {
            paramBatch.add(Arrays.asList(version, now, each));
        }
        final int[] rowCount = writer.batchUpdate(conn, swapSql, paramBatch);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0 ? version : null;
    }

    // ---- delete ----

    @Override
    public final void delete(final Connection conn, final K key) {
        writer.update(conn, deleteSql, Arrays.asList(key));
    }

    @Override
    public final void batchDelete(final Connection conn, final List<K> keys) {
        final Collection<Iterable<?>> paramBatch = new ArrayList<>(keys.size());
        for (K each: keys) {
            paramBatch.add(Arrays.asList(each));
        }
        writer.batchUpdate(conn, deleteSql, paramBatch);
    }

    // ---- remove (requires old version) ----

    @Override
    public final void remove(final Connection conn, final K key, final long version) {
        writer.update(conn, condDeleteSql, Arrays.asList(key, version));
    }

    @Override
    public final void batchRemove(final Connection conn, final List<K> keys, final long version) {
        final Collection<Iterable<?>> paramBatch = new ArrayList<>(keys.size());
        for (K each: keys) {
            paramBatch.add(Arrays.asList(each, version));
        }
        writer.batchUpdate(conn, condDeleteSql, paramBatch);
    }

    @Override
    public final void batchRemove(final Connection conn, final Map<K, Long> keys) {
        final Collection<Iterable<?>> paramBatch = new ArrayList<>(keys.size());
        for (Entry<K, Long> each: keys.entrySet()) {
            paramBatch.add(Arrays.asList(each.getKey(), each.getValue()));
        }
        writer.batchUpdate(conn, condDeleteSql, paramBatch);
    }

}
