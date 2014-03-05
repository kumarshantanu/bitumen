package starfish;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import starfish.helper.JdbcUtil;
import starfish.helper.Util;
import starfish.type.KeyValueVersion;
import starfish.type.TableMetadata;

public class GenericOpsWrite<K, V> implements IOpsWrite<K, V> {

    public static final String[] upsertFormat = {
            "UPDATE $tableName SET $valueColname = ?, $versionColname = ?, $timestampColname = ? WHERE $keyColname = ?", // val, version, timestamp, key
            "INSERT INTO $tableName ($keyColname, $valueColname, $versionColname, $timestampColname) VALUES (?, ?, ?, ?)" // key, val, version, timestamp
    };

    public static final String
    swapFormat       = "UPDATE $tableName SET $valueColname = ?, $versionColname = ?, $timestampColname = ?"
            + " WHERE $keyColname = ? AND $versionColname = ?", // val, ver, timestamp, key, old-ver
    touchFormat      = "UPDATE $tableName SET $versionColname = ?, $timestampColname = ? WHERE $keyColname = ?", // version, timestamp, key
    versionFormat    = "SELECT $versionColname FROM $tableName WHERE $keyColname = ?",    // key
    condFetchFormat  = "SELECT $valueColname FROM $tableName WHERE $keyColname = ? AND $versionColname = ?", // key, old-version
    fetchFormat      = "SELECT $valueColname FROM $tableName WHERE $keyColname = ?", // key
    deleteFormat     = "DELETE FROM $tableName WHERE $keyColname = ?",  // key
    condDeleteFormat = "DELETE FROM $tableName WHERE $keyColname = ? AND $versionColname = ?"; // key, old-version

    public final String[] upsertSql;
    public final String swapSql, touchSql, versionSql, condFetchSql, fetchSql, deleteSql, condDeleteSql;

    public GenericOpsWrite(final TableMetadata meta) {
        this.upsertSql = new String[] { meta.groovyReplace(upsertFormat[0]), meta.groovyReplace(upsertFormat[1]) };
        this.swapSql       = meta.groovyReplace(swapFormat);
        this.touchSql      = meta.groovyReplace(touchFormat);
        this.versionSql    = meta.groovyReplace(versionFormat);
        this.condFetchSql  = meta.groovyReplace(condFetchFormat);
        this.fetchSql      = meta.groovyReplace(fetchFormat);
        this.deleteSql     = meta.groovyReplace(deleteFormat);
        this.condDeleteSql = meta.groovyReplace(condDeleteFormat);
    }

    // ---- save, regardless of whether they already exist ----

    public long save(Connection conn, K key, V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        int rows = JdbcUtil.update(conn, upsertSql[0], new Object[] { value, version, now, key });
        if (rows == 0) {
            JdbcUtil.update(conn, upsertSql[1], new Object[] { key, value, version, now });
        }
        return version;
    }

    public long batchSave(Connection conn, Map<K, V> pairs) {
        final Timestamp now = Util.now();
        final long version = Util.newVersion();
        Object[][] updateArgsArray = new Object[pairs.size()][];
        int i = 0;
        for (Entry<K, V> each: pairs.entrySet()) { 
            updateArgsArray[i++] = new Object[] { each.getValue(), version, now, each.getKey() };
        }
        int[] rows = JdbcUtil.batchUpdate(conn, upsertSql[0], updateArgsArray);
        updateArgsArray = null;
        int toInsert = 0;
        for (int j = 0; j < rows.length; j++) {
            if (rows[j] == 0) {
                toInsert++;
            }
        }
        if (toInsert > 0) {
            Object[][] insertArgsArray = new Object[toInsert][];
            i = 0;
            for (Entry<K, V> each: pairs.entrySet()) {
                if (rows[i] == 0) {
                    insertArgsArray[i] = new Object[] { each.getKey(), each.getValue(), version, now };
                }
            }
            JdbcUtil.batchUpdate(conn, upsertSql[1], insertArgsArray);
        }
        return version;
    }

    // ---- swap (requires old version) ----

    public Long swap(Connection conn, K key, V value, long version) {
        final long tmpVersion = Util.newVersion();
        final long newVersion = version == tmpVersion? version + 1: tmpVersion;
        int rowCount = JdbcUtil.update(conn, swapSql, new Object[] { value, newVersion, Util.now(), key, version });
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
                argsArray[i] = new Object[] { each.getValue(), newVersion, now,
                        each.getKey(), version };
            }
        }
        final int[] rowCount = JdbcUtil.batchUpdate(conn, swapSql, argsArray);
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
                argsArray[i] = new Object[] { each.value, newVersion, now,
                        each.key, each.version };
            }
        }
        final int[] rowCount = JdbcUtil.batchUpdate(conn, swapSql, argsArray);
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
        final int rowCount = JdbcUtil.update(conn, touchSql, new Object[] { version, now, key });
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
        final int[] rowCount = JdbcUtil.batchUpdate(conn, swapSql, argsArray);
        int totalRowCount = 0;
        for (int j = 0; j < rowCount.length; j++) {
            totalRowCount += rowCount[j];
        }
        return totalRowCount > 0? version: null;
    }

    // ---- delete ----

    public void delete(Connection conn, K key) {
        JdbcUtil.update(conn, deleteSql, new Object[] { key });
    }

    public void batchDelete(Connection conn, List<K> keys) {
        Object[][] argsArray = new Object[keys.size()][];
        int i = 0;
        for (K each: keys) {
            argsArray[i++] = new Object[] { each };
        }
        JdbcUtil.batchUpdate(conn, deleteSql, argsArray);
    }

    // ---- remove (requires old version) ----

    public void remove(Connection conn, K key, long version) {
        JdbcUtil.update(conn, condDeleteSql, new Object[] { key, version });
    }

    public void batchRemove(Connection conn, List<K> keys, long version) {
        Object[][] argsArray = new Object[keys.size()][];
        int i = 0;
        for (K each: keys) {
            argsArray[i++] = new Object[] { each, version };
        }
        JdbcUtil.batchUpdate(conn, condDeleteSql, argsArray);
    }

    public void batchRemove(Connection conn, Map<K, Long> keys) {
        Object[][] argsArray = new Object[keys.size()][];
        int i = 0;
        for (Entry<K, Long> each: keys.entrySet()) {
            argsArray[i++] = new Object[] { each.getKey(), each.getValue() };
        }
        JdbcUtil.batchUpdate(conn, condDeleteSql, argsArray);
    }

}
