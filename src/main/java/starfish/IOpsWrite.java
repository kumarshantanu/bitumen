package starfish;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

import starfish.type.KeyValueVersion;

public interface IOpsWrite<K, V> {

    // ---- save, regardless of whether they already exist ----

    public long save(Connection conn, K key, V value);

    public long batchSave(Connection conn, Map<K, V> pairs);

    // ---- swap (requires old version) ----

    public Long swap(Connection conn, K key, V value, long version);

    public Long batchSwap(Connection conn, Map<K, V> pairs, long version);

    public Long batchSwap(Connection conn, List<KeyValueVersion<K, V>> triplets);

    // ---- touch (update version) ----

    public Long touch(Connection conn, K key);

    public Long batchTouch(Connection conn, List<K> keys);

    // ---- delete ----

    public void delete(Connection conn, K key);

    public void batchDelete(Connection conn, List<K> keys);

    // ---- remove (requires old version) ----

    public void remove(Connection conn, K key, long version);

    public void batchRemove(Connection conn, List<K> keys, long version);

    public void batchRemove(Connection conn, Map<K, Long> keys);

}
