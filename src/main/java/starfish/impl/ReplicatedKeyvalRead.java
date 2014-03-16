package starfish.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import starfish.KeyvalRead;
import starfish.helper.ConnectionActivity;
import starfish.helper.JdbcUtil;
import starfish.helper.Util;
import starfish.type.TableMetadata;
import starfish.type.ValueVersion;

public class ReplicatedKeyvalRead<K, V> implements KeyvalRead<K, V> {

    public final ReplicationSlavesPointer slavesPointer;
    public final KeyvalRead<K, V> reader;

    public ReplicatedKeyvalRead(final TableMetadata meta, Class<K> keyClass, Class<V> valClass,
            ReplicationSlavesPointer slavesPointer) {
        this(meta, keyClass, valClass, slavesPointer, new DefaultKeyvalRead<K, V>(meta, keyClass, valClass));
    }

    public ReplicatedKeyvalRead(final TableMetadata meta, Class<K> keyClass, Class<V> valClass,
            ReplicationSlavesPointer slavesPointer, KeyvalRead<K, V> orig) {
        this.slavesPointer = slavesPointer;
        this.reader = orig;
    }

    private volatile int index = 0;
    private DataSource nextSlaveDataSource() { // this uses an approximate counter for efficiency
        final List<DataSource> ds = slavesPointer.getDataSources();
        if (ds == null || ds.isEmpty()) {
            return null;
        } else if (ds.size() == 1) {
            return ds.get(0);
        }
        int i = ++index;
        if (i >= ds.size()) {
            i = 0;
            index = 0;
        }
        return ds.get(i);
    }

    // ---- contains ----

    public Long contains(Connection conn, K key) {
        return reader.contains(conn, key);
    }

    public List<Long> batchContains(Connection conn, List<K> keys) {
        return reader.batchContains(conn, keys);
    }

    // ---- containsVersion (requires old version) ----

    public boolean containsVersion(Connection conn, K key, long version) {
        return reader.containsVersion(conn, key, version);
    }

    public Map<K, Boolean> batchContainsVersion(Connection conn, Map<K, Long> keyVersions) {
        return reader.batchContainsVersion(conn, keyVersions);
    }

    // ---- read ----

    public V consistentRead(final Connection conn, DataSource slave, final K key) {
        final Long latest = contains(conn, key);
        if (latest == null) {
            return null;
        }
        V copy = JdbcUtil.withConnection(slave, new ConnectionActivity<V>() {
            public V execute(Connection conn) {
                return reader.readForVersion(conn, key, latest);
            }
        });
        return copy == null? reader.read(conn, key): copy;
    }

    public V read(Connection conn, K key) {
        final DataSource slave = nextSlaveDataSource();
        return slave == null? reader.read(conn, key): consistentRead(conn, slave, key);
    }

    public Map<K, V> consistentBatchRead(Connection conn, DataSource slave, final List<K> keys) {
        final List<Long> latest = batchContains(conn, keys);
        if (Util.areAllNull(latest)) {
            List<V> data = new ArrayList<V>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                data.add(null);
            }
            return Util.zipmap(keys, data);
        }
        final Map<K, V> copy = JdbcUtil.withConnection(slave, new ConnectionActivity<Map<K, V>>() {
            public Map<K, V> execute(Connection conn) {
                return reader.batchRead(conn, keys);
            }
        });
        final List<K> missing = new ArrayList<K>(keys);
        for (K key: copy.keySet()) {
            missing.remove(key);
        }
        if (!missing.isEmpty()) {
            final Map<K, V> master = reader.batchRead(conn, missing);
            copy.putAll(master);
        }
        return copy;
    }

    public Map<K, V> batchRead(Connection conn, List<K> keys) {
        final DataSource slave = nextSlaveDataSource();
        return slave == null? reader.batchRead(conn, keys): consistentBatchRead(conn, slave, keys);
    }

    // ---- readVersion (requires old version) ----

    public V consistentReadVersion(Connection conn, DataSource slave, final K key, final long version) {
        V copy = JdbcUtil.withConnection(slave, new ConnectionActivity<V>() {
            public V execute(Connection conn) {
                return reader.readForVersion(conn, key, version);
            }
        });
        return copy == null? reader.readForVersion(conn, key, version): copy;
    }

    public V readForVersion(Connection conn, K key, long version) {
        final DataSource slave = nextSlaveDataSource();
        return slave == null? reader.readForVersion(conn, key, version): consistentReadVersion(conn, slave, key, version);
    }

    public Map<K, V> consistentBatchReadVersion(Connection conn, DataSource slave, final Map<K, Long> keyVersions) {
        final Map<K, V> copy = JdbcUtil.withConnection(slave, new ConnectionActivity<Map<K, V>>() {
            public Map<K, V> execute(Connection conn) {
                return reader.batchReadForVersion(conn, keyVersions);
            }
        });
        final Map<K, Long> missing = new LinkedHashMap<K, Long>(keyVersions);
        for (K key: copy.keySet()) {
            missing.remove(key);
        }
        if (!missing.isEmpty()) {
            final Map<K, V> master = reader.batchReadForVersion(conn, missing);
            copy.putAll(master);
        }
        return copy;
    }

    public Map<K, V> batchReadForVersion(Connection conn, Map<K, Long> keyVersions) {
        final DataSource slave = nextSlaveDataSource();
        return slave == null? reader.batchReadForVersion(conn, keyVersions):
            consistentBatchReadVersion(conn, slave, keyVersions);
    }

    // ---- readAll ----

    public ValueVersion<V> consistentReadAll(Connection conn, DataSource slave, final K key) {
        final Long latest = contains(conn, key);
        if (latest == null) {
            return null;
        }
        ValueVersion<V> copy = JdbcUtil.withConnection(slave,
                new ConnectionActivity<ValueVersion<V>>() {
                    public ValueVersion<V> execute(Connection conn) {
                        return reader.readAll(conn, key);
                    }
                });
        return (copy == null || !copy.version.equals(latest)) ? reader.readAll(conn, key) : copy;
    }

    public ValueVersion<V> readAll(Connection conn, K key) {
        final DataSource slave = nextSlaveDataSource();
        return slave == null? reader.readAll(conn, key): consistentReadAll(conn, slave, key);
    }

    public Map<K, ValueVersion<V>> consistentBatchReadAll(Connection conn, DataSource slave, List<K> keys) {
        final List<Long> latest = batchContains(conn, keys);
        final Map<K, Long> keyVersions = Util.zipmap(keys, latest);
        final Map<K, V> slaveKeyVals = JdbcUtil.withConnection(slave, new ConnectionActivity<Map<K, V>>() {
            public Map<K, V> execute(Connection slaveConn) {
                return reader.batchReadForVersion(slaveConn, keyVersions);
            }
        });
        final Map<K, Long> missingKeyVersions = new HashMap<K, Long>(keyVersions);
        for (K key: slaveKeyVals.keySet()) {
            missingKeyVersions.remove(key);
        }
        if (!missingKeyVersions.isEmpty()) {
            final Map<K, V> masterKeyVals = reader.batchReadForVersion(conn, missingKeyVersions);
            slaveKeyVals.putAll(masterKeyVals);
        }
        final Map<K, ValueVersion<V>> result = new LinkedHashMap<K, ValueVersion<V>>();
        final int len = keys.size();
        for (int i = 0; i < len; i++) {
            final K key = keys.get(i);
            final Long version = latest.get(i);
            result.put(key, version==null? null: new ValueVersion<V>(slaveKeyVals.get(key), version));
        }
        return result;
    }

    public Map<K, ValueVersion<V>> batchReadAll(Connection conn, List<K> keys) {
        final DataSource slave = nextSlaveDataSource();
        return slave == null? reader.batchReadAll(conn, keys): consistentBatchReadAll(conn, slave, keys);
    }

}
