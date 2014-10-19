package springer.jdbc.kv;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface IKeyvalRead<K, V> {

    // ---- contains ----

    public Long contains(Connection conn, K key);

    public List<Long> batchContains(Connection conn, List<K> keys);

    // ---- containsVersion (requires old version) ----

    public boolean containsVersion(Connection conn, K key, long version);

    public Map<K, Boolean> batchContainsVersion(Connection conn, Map<K, Long> keyVersions);

    // ---- read ----

    public V read(Connection conn, K key);

    public Map<K, V> batchRead(Connection conn, List<K> keys);

    // ---- readVersion (requires old version) ----

    public V readForVersion(Connection conn, K key, long version);

    public Map<K, V> batchReadForVersion(Connection conn, Map<K, Long> keyVersions);

    // ---- readAll ----

    public ValueVersion<V> readAll(Connection conn, K key);

    public Map<K, ValueVersion<V>> batchReadAll(Connection conn, List<K> keys);

}
