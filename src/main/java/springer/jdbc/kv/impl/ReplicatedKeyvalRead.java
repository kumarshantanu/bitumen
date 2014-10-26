package springer.jdbc.kv.impl;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import springer.jdbc.impl.IConnectionActivity;
import springer.jdbc.impl.JdbcUtil;
import springer.jdbc.kv.IKeyvalRead;
import springer.jdbc.kv.ValueVersion;
import springer.util.Util;

/**
 * A master-slave replication aware implementation of {@link IKeyvalRead}. The reads are directed to slaves in a
 * Round-robin fashion.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class ReplicatedKeyvalRead<K, V> implements IKeyvalRead<K, V> {

    /** Source of slave data sources. */
    private final IReplicationSlavesPointer slavesPointer;

    /** JDBC reader. */
    private final IKeyvalRead<K, V> reader;

    /**
     * Construct instance using required arguments to infer defaults.
     * @param meta         table meta data
     * @param keyClass     key class
     * @param valClass     value class
     * @param slavesSource source of slave data sources
     */
    public ReplicatedKeyvalRead(final TableMetadata meta, final Class<K> keyClass, final Class<V> valClass,
            final IReplicationSlavesPointer slavesSource) {
        this(slavesSource, new DefaultKeyvalRead<K, V>(meta, keyClass, valClass));
    }

    /**
     * Construct instance using all required arguments.
     * @param slavesSource source of slave data sources
     * @param orig         key-value reader to actually connect and read
     */
    public ReplicatedKeyvalRead(final IReplicationSlavesPointer slavesSource, final IKeyvalRead<K, V> orig) {
        this.slavesPointer = slavesSource;
        this.reader = orig;
    }

    /** Index that loops from 0 until slave-count, at which point it rolls over to 0. */
    private volatile int index = 0;

    /**
     * Obtain the next slave data source in round-robin fashion.
     * @return next slave {@link DataSource} in Round-robin fashion.
     */
    private DataSource nextSlaveDataSource() { // this uses an approximate (but lock-free) counter for efficiency
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

    @Override
    public final Long contains(final Connection conn, final K key) {
        return reader.contains(conn, key);
    }

    @Override
    public final List<Long> batchContains(final Connection conn, final List<K> keys) {
        return reader.batchContains(conn, keys);
    }

    // ---- containsVersion (requires old version) ----

    @Override
    public final boolean containsVersion(final Connection conn, final K key, final long version) {
        return reader.containsVersion(conn, key, version);
    }

    @Override
    public final Map<K, Boolean> batchContainsVersion(final Connection conn, final Map<K, Long> keyVersions) {
        return reader.batchContainsVersion(conn, keyVersions);
    }

    // ---- read ----

    /**
     * Read consistently across master and slave.
     * @param  conn  JDBC connection
     * @param  slave slave {@link DataSource}
     * @param  key   key to find
     * @return       corresponding value of the key (<tt>null</tt> if not key found)
     */
    public final V consistentRead(final Connection conn, final DataSource slave, final K key) {
        final Long latest = contains(conn, key);
        if (latest == null) {
            return null;
        }
        V copy = JdbcUtil.withConnection(slave, new IConnectionActivity<V>() {
            @Override
            public V execute(final Connection conn) {
                return reader.readForVersion(conn, key, latest);
            }
        });
        if (copy == null) {
            return reader.read(conn, key);
        } else {
            return copy;
        }
    }

    @Override
    public final V read(final Connection conn, final K key) {
        final DataSource slave = nextSlaveDataSource();
        if (slave == null) {
            return reader.read(conn, key);
        } else {
            return consistentRead(conn, slave, key);
        }
    }

    /**
     * Read batch consistently across master and slave.
     * @param  conn  JDBC connection
     * @param  slave slave {@link DataSource} instance
     * @param  keys  keys to find
     * @return       map of keys and corresponding values (<tt>null</tt> if key not found)
     */
    public final Map<K, V> consistentBatchRead(final Connection conn, final DataSource slave, final List<K> keys) {
        final List<Long> latest = batchContains(conn, keys);
        if (Util.areAllNull(latest)) {
            List<V> data = new ArrayList<V>(keys.size());
            for (int i = 0; i < keys.size(); i++) {
                data.add(null);
            }
            return Util.zipmap(keys, data);
        }
        final Map<K, V> copy = JdbcUtil.withConnection(slave, new IConnectionActivity<Map<K, V>>() {
            @Override
            public Map<K, V> execute(final Connection conn) {
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

    @Override
    public final Map<K, V> batchRead(final Connection conn, final List<K> keys) {
        final DataSource slave = nextSlaveDataSource();
        if (slave == null) {
            return reader.batchRead(conn, keys);
        } else {
            return consistentBatchRead(conn, slave, keys);
        }
    }

    // ---- readVersion (requires old version) ----

    /**
     * Read value consistent across master and slave.
     * @param  conn    JDBC connection
     * @param  slave   slave {@link DataSource} instance
     * @param  key     key to find
     * @param  version version to match for specified key
     * @return         corresponding value of the key
     */
    public final V consistentReadVersion(final Connection conn, final DataSource slave, final K key, final long version) {
        final V copy = JdbcUtil.withConnection(slave, new IConnectionActivity<V>() {
            @Override
            public V execute(final Connection conn) {
                return reader.readForVersion(conn, key, version);
            }
        });
        if (copy == null) {
            return reader.readForVersion(conn, key, version);
        } else {
            return copy;
        }
    }

    @Override
    public final V readForVersion(final Connection conn, final K key, final long version) {
        final DataSource slave = nextSlaveDataSource();
        if (slave == null) {
            return reader.readForVersion(conn, key, version);
        } else {
            return consistentReadVersion(conn, slave, key, version);
        }
    }

    /**
     * Read values consistently across master and slave.
     * @param  conn        JDBC connection
     * @param  slave       slave {@link DataSource} instance
     * @param  keyVersions map of keys (to find) and corresponding versions (to match)
     * @return             map of keys and corresponding values (<tt>null</tt> for each value when there was no match)
     */
    public final Map<K, V> consistentBatchReadVersion(final Connection conn, final DataSource slave, final Map<K, Long> keyVersions) {
        final Map<K, V> copy = JdbcUtil.withConnection(slave, new IConnectionActivity<Map<K, V>>() {
            @Override
            public Map<K, V> execute(final Connection conn) {
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

    @Override
    public final Map<K, V> batchReadForVersion(final Connection conn, final Map<K, Long> keyVersions) {
        final DataSource slave = nextSlaveDataSource();
        if (slave == null) {
            return reader.batchReadForVersion(conn, keyVersions);
        } else {
            return consistentBatchReadVersion(conn, slave, keyVersions);
        }
    }

    // ---- readAll ----

    /**
     * Read value and version for specified key consistently across master and slave.
     * @param  conn  JDBC connection
     * @param  slave slave {@link DataSource} instance
     * @param  key   key to find
     * @return       corresponding value and version (<tt>null</tt> when key not found)
     */
    public final ValueVersion<V> consistentReadAll(final Connection conn, final DataSource slave, final K key) {
        final Long latest = contains(conn, key);
        if (latest == null) {
            return null;
        }
        final ValueVersion<V> copy = JdbcUtil.withConnection(slave,
                new IConnectionActivity<ValueVersion<V>>() {
                   @Override
                    public ValueVersion<V> execute(final Connection conn) {
                        return reader.readAll(conn, key);
                    }
                });
        if (copy == null || !copy.version.equals(latest)) {
            return reader.readAll(conn, key);
        } else {
            return copy;
        }
    }

    @Override
    public final ValueVersion<V> readAll(final Connection conn, final K key) {
        final DataSource slave = nextSlaveDataSource();
        if (slave == null) {
            return reader.readAll(conn, key);
        } else {
            return consistentReadAll(conn, slave, key);
        }
    }

    /**
     * Read value and version of each key consistently across master and slave.
     * @param  conn  JDBC connection
     * @param  slave slave {@link DataSource} instance
     * @param  keys  keys to find
     * @return       map of keys to value-version tuple (<tt>null</tt> for each key that was not found)
     */
    public final Map<K, ValueVersion<V>> consistentBatchReadAll(final Connection conn, final DataSource slave,
            final List<K> keys) {
        final List<Long> latest = batchContains(conn, keys);
        final Map<K, Long> keyVersions = Util.zipmap(keys, latest);
        final Map<K, V> slaveKeyVals = JdbcUtil.withConnection(slave, new IConnectionActivity<Map<K, V>>() {
            @Override
            public Map<K, V> execute(final Connection slaveConn) {
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
            result.put(key, version == null ? null : new ValueVersion<V>(slaveKeyVals.get(key), version));
        }
        return result;
    }

    @Override
    public final Map<K, ValueVersion<V>> batchReadAll(final Connection conn, final List<K> keys) {
        final DataSource slave = nextSlaveDataSource();
        if (slave == null) {
            return reader.batchReadAll(conn, keys);
        } else {
            return consistentBatchReadAll(conn, slave, keys);
        }
    }

}
