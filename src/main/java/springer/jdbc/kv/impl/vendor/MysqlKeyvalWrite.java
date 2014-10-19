package springer.jdbc.kv.impl.vendor;

import java.sql.Connection;
import java.sql.Timestamp;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import springer.jdbc.IJdbcWrite;
import springer.jdbc.impl.DefaultJdbcWrite;
import springer.jdbc.kv.KeyValueVersion;
import springer.jdbc.kv.IKeyvalWrite;
import springer.jdbc.kv.impl.DefaultKeyvalWrite;
import springer.jdbc.kv.impl.TableMetadata;
import springer.util.Util;

public class MysqlKeyvalWrite<K, V> implements IKeyvalWrite<K, V> {

    public static final String
    upsertFormat = "INSERT INTO $tableName ($keyColname, $valueColname, $versionColname, $createTimestampColname, $updateTimestampColname)"
            + " VALUES (?, ?, ?, $timestampValuePlaceholder, $timestampValuePlaceholder)"
            + " ON DUPLICATE KEY UPDATE $valueColname = ?, $versionColname = ?, $updateTimestampColname = $timestampValuePlaceholder";

    public final String upsertSql;

    public final IJdbcWrite writer;

    public final DefaultKeyvalWrite<K, V> generic;
    public final boolean populateTimestamp;

    public MysqlKeyvalWrite(TableMetadata meta, boolean useMySQLTimestamp) {
        this(meta, useMySQLTimestamp, new DefaultJdbcWrite());
    }

    public MysqlKeyvalWrite(TableMetadata meta, boolean useMySQLTimestamp, IJdbcWrite writer) {
        this.writer = writer;
        this.generic = new DefaultKeyvalWrite<K, V>(meta);
        final String template = meta.groovyReplaceKeep(upsertFormat);
        this.upsertSql = Util.groovyReplace(template,
                Collections.singletonMap("timestampValuePlaceholder", useMySQLTimestamp? "NOW()": "?"), true);
        this.populateTimestamp = !useMySQLTimestamp;
    }

    public long insert(Connection conn, K key, V value) {
        return generic.insert(conn, key, value);
    }

    public long batchInsert(Connection conn, Map<K, V> pairs) {
        return generic.batchInsert(conn, pairs);
    }

    // ---- save, regardless of whether they already exist ----

    public long save(Connection conn, K key, V value) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        writer.update(conn, upsertSql, populateTimestamp?
                new Object[] { key, value, version, now, now, value, version, now }:
                    new Object[] { key, value, version, value, version });
        return version;
    }

    public long batchSave(Connection conn, Map<K, V> pairs) {
        final long version = Util.newVersion();
        final Timestamp now = Util.now();
        final Object[][] argsArray = new Object[pairs.size()][];
        int i = 0;
        for (Entry<K, V> entry: pairs.entrySet()) {
            final K key = entry.getKey();
            final V value = entry.getValue();
            argsArray[i++] = populateTimestamp?
                    new Object[] { key, value, version, now, now, value, version, now }:
                        new Object[] { key, value, version, value, version };
        }
        writer.batchUpdate(conn, upsertSql, argsArray);
        return version;
    }

    // ---- swap (requires old version) ----

    public Long swap(Connection conn, K key, V value, long version) {
        return generic.swap(conn, key, value, version);
    }

    public Long batchSwap(Connection conn, Map<K, V> pairs, long version) {
        return generic.batchSwap(conn, pairs, version);
    }

    public Long batchSwap(Connection conn, List<KeyValueVersion<K, V>> triplets) {
        return generic.batchSwap(conn, triplets);
    }

    // ---- touch (update version) ----

    public Long touch(Connection conn, K key) {
        return generic.touch(conn, key);
    }

    public Long batchTouch(Connection conn, List<K> keys) {
        return generic.batchTouch(conn, keys);
    }

    // ---- delete ----

    public void delete(Connection conn, K key) {
        generic.delete(conn, key);
    }

    public void batchDelete(Connection conn, List<K> keys) {
        generic.batchDelete(conn, keys);
    }

    // ---- remove (requires old version) ----

    public void remove(Connection conn, K key, long version) {
        generic.remove(conn, key, version);
    }

    public void batchRemove(Connection conn, List<K> keys, long version) {
        generic.batchRemove(conn, keys, version);
    }

    public void batchRemove(Connection conn, Map<K, Long> keys) {
        generic.batchRemove(conn, keys);
    }

}
