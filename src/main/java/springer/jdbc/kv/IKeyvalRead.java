package springer.jdbc.kv;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface IKeyvalRead<K, V> {

    // ---- contains ----

    Long contains(Connection conn, K key);

    List<Long> batchContains(Connection conn, List<K> keys);

    // ---- containsVersion (requires old version) ----

    boolean containsVersion(Connection conn, K key, long version);

    Map<K, Boolean> batchContainsVersion(Connection conn, Map<K, Long> keyVersions);

    // ---- read ----

    V read(Connection conn, K key);

    Map<K, V> batchRead(Connection conn, List<K> keys);

    // ---- readVersion (requires old version) ----

    V readForVersion(Connection conn, K key, long version);

    Map<K, V> batchReadForVersion(Connection conn, Map<K, Long> keyVersions);

    // ---- readAll ----

    ValueVersion<V> readAll(Connection conn, K key);

    Map<K, ValueVersion<V>> batchReadAll(Connection conn, List<K> keys);

}
