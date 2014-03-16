package starfish.impl;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import starfish.JdbcWrite;
import starfish.KeyvalWrite;
import starfish.helper.Util;
import starfish.type.KeyValueVersion;
import starfish.type.TableMetadata;

public class DefaultKeyvalWrite<K, V> implements KeyvalWrite<K, V> {

    public static final String
    insertFormat     = "INSERT INTO $tableName ($keyColname, $valueColname, $versionColname, $createTimestampColname,"
            + " $updateTimestampColname) VALUES (?, ?, ?, ?, ?)", // key, val, version, timestamp[1,2]
    updateFormat     = "UPDATE $tableName SET $valueColname = ?, $versionColname = ?, $updateTimestampColname = ?"
            + " WHERE $keyColname = ?", // val, version, timestamp, key
    swapFormat       = "UPDATE $tableName SET $valueColname = ?, $versionColname = ?, $updateTimestampColname = ?"
            + " WHERE $keyColname = ? AND $versionColname = ?", // val, ver, timestamp, key, old-ver
    touchFormat      = "UPDATE $tableName SET $versionColname = ?, $updateTimestampColname = ? WHERE $keyColname = ?", // version, timestamp, key
    versionFormat    = "SELECT $versionColname FROM $tableName WHERE $keyColname = ?",    // key
    deleteFormat     = "DELETE FROM $tableName WHERE $keyColname = ?",  // key
    condDeleteFormat = "DELETE FROM $tableName WHERE $keyColname = ? AND $versionColname = ?"; // key, old-version

    public final String insertSql, updateSql, swapSql, touchSql, versionSql, deleteSql, condDeleteSql;

    public final JdbcWrite writer;

    public DefaultKeyvalWrite(final TableMetadata meta) {
        this(meta, new DefaultJdbcWrite());
    }

    public DefaultKeyvalWrite(final TableMetadata meta, JdbcWrite writer) {
        this.insertSql     = meta.groovyReplace(insertFormat);
        this.updateSql     = meta.groovyReplace(updateFormat);
        this.swapSql       = meta.groovyReplace(swapFormat);
        this.touchSql      = meta.groovyReplace(touchFormat);
        this.versionSql    = meta.groovyReplace(versionFormat);
        this.deleteSql     = meta.groovyReplace(deleteFormat);
        this.condDeleteSql = meta.groovyReplace(condDeleteFormat);
        this.writer = writer;
    }

    // ----- insert -----

    public long insert(Connection conn, K key, V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        writer.update(conn, insertSql, new Object[] { key, value, version, now, now });
        return version;
    }

    public long batchInsert(Connection conn, Map<K, V> pairs) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        final Object[][] insertArgsArray = new Object[pairs.size()][];
        int i = 0;
        for (Entry<K, V> each: pairs.entrySet()) {
            insertArgsArray[i++] = new Object[] { each.getKey(), each.getValue(), version, now, now };
        }
        writer.batchUpdate(conn, insertSql, insertArgsArray);
        return version;
    }

    // ---- save, regardless of whether they already exist ----

    public long save(Connection conn, K key, V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        int rows = writer.update(conn, updateSql, new Object[] { value, version, now, key });
        if (rows == 0) {
            writer.update(conn, insertSql, new Object[] { key, value, version, now, now });
        }
        return version;
    }

    public long batchSave(Connection conn, Map<K, V> pairs) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        int[] rows = null;
        {
            final Object[][] updateArgsArray = new Object[pairs.size()][];
            int i = 0;
            for (Entry<K, V> each : pairs.entrySet()) {
                updateArgsArray[i++] = new Object[] { each.getValue(), version, now, each.getKey() };
            }
            rows = writer.batchUpdate(conn, updateSql, updateArgsArray);
        }
        int toInsert = 0;
        for (int i = 0; i < rows.length; i++) {
            if (rows[i] == 0) {
                toInsert++;
            }
        }
        if (toInsert > 0) {
            Object[][] insertArgsArray = new Object[toInsert][];
            int i = 0, j = 0;
            for (Entry<K, V> each: pairs.entrySet()) {
                if (rows[i++] == 0) {
                    insertArgsArray[j++] = new Object[] { each.getKey(), each.getValue(), version, now, now };
                }
            }
            writer.batchUpdate(conn, insertSql, insertArgsArray);
        }
        return version;
    }

    // ---- swap (requires old version) ----

    public Long swap(Connection conn, K key, V value, long version) {
        final long tmpVersion = Util.newVersion();
        final long newVersion = version == tmpVersion? version + 1: tmpVersion;
        int rowCount = writer.update(conn, swapSql, new Object[] { value, newVersion, Util.now(), key, version });
        return rowCount > 0? newVersion: null;
    }

    public Long batchSwap(Connection conn, Map<K, V> pairs, long version) {
        final Timestamp now = Util.now();
        final long tmpVersion = Util.newVersion();
        final long newVersion = version == tmpVersion? version + 1: tmpVersion;
        Object[][] argsArray = new Object[pairs.size()][];
        {
            int i = 0;
            for (Entry<K, V> each : pairs.entrySet()) {
                argsArray[i++] = new Object[] { each.getValue(), newVersion, now,
                        each.getKey(), version };
            }
        }
        final int[] rowCount = writer.batchUpdate(conn, swapSql, argsArray);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0? newVersion: null;
    }

    public Long batchSwap(Connection conn, List<KeyValueVersion<K, V>> triplets) {
        final Timestamp now = Util.now();
        final long newVersion = Util.newVersion();
        Object[][] argsArray = new Object[triplets.size()][];
        {
            int i = 0;
            for (KeyValueVersion<K, V> each : triplets) {
                argsArray[i++] = new Object[] { each.value, newVersion, now,
                        each.key, each.version };
            }
        }
        final int[] rowCount = writer.batchUpdate(conn, swapSql, argsArray);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0? newVersion: null;
    }

    // ---- touch (update version) ----

    public Long touch(Connection conn, K key) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        final int rowCount = writer.update(conn, touchSql, new Object[] { version, now, key });
        return rowCount > 0? version: null;
    }

    public Long batchTouch(Connection conn, List<K> keys) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        Object[][] argsArray = new Object[keys.size()][];
        {
            int i = 0;
            for (K each : keys) {
                argsArray[i++] = new Object[] { version, now, each };
            }
        }
        final int[] rowCount = writer.batchUpdate(conn, swapSql, argsArray);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0? version: null;
    }

    // ---- delete ----

    public void delete(Connection conn, K key) {
        writer.update(conn, deleteSql, new Object[] { key });
    }

    public void batchDelete(Connection conn, List<K> keys) {
        Object[][] argsArray = new Object[keys.size()][];
        int i = 0;
        for (K each: keys) {
            argsArray[i++] = new Object[] { each };
        }
        writer.batchUpdate(conn, deleteSql, argsArray);
    }

    // ---- remove (requires old version) ----

    public void remove(Connection conn, K key, long version) {
        writer.update(conn, condDeleteSql, new Object[] { key, version });
    }

    public void batchRemove(Connection conn, List<K> keys, long version) {
        Object[][] argsArray = new Object[keys.size()][];
        int i = 0;
        for (K each: keys) {
            argsArray[i++] = new Object[] { each, version };
        }
        writer.batchUpdate(conn, condDeleteSql, argsArray);
    }

    public void batchRemove(Connection conn, Map<K, Long> keys) {
        Object[][] argsArray = new Object[keys.size()][];
        int i = 0;
        for (Entry<K, Long> each: keys.entrySet()) {
            argsArray[i++] = new Object[] { each.getKey(), each.getValue() };
        }
        writer.batchUpdate(conn, condDeleteSql, argsArray);
    }

}
