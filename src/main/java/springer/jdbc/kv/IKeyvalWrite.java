package springer.jdbc.kv;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

public interface IKeyvalWrite<K, V> {

    // ---- insert (fails if key already exists) ----

    long insert(Connection conn, K key, V value);

    long batchInsert(Connection conn, Map<K, V> pairs);

    // ---- save, regardless of whether they already exist ----

    long save(Connection conn, K key, V value);

    long batchSave(Connection conn, Map<K, V> pairs);

    // ---- swap (requires old version) ----

    Long swap(Connection conn, K key, V value, long version);

    Long batchSwap(Connection conn, Map<K, V> pairs, long version);

    Long batchSwap(Connection conn, List<KeyValueVersion<K, V>> triplets);

    // ---- touch (update version) ----

    Long touch(Connection conn, K key);

    Long batchTouch(Connection conn, List<K> keys);

    // ---- delete ----

    void delete(Connection conn, K key);

    void batchDelete(Connection conn, List<K> keys);

    // ---- remove (requires old version) ----

    void remove(Connection conn, K key, long version);

    void batchRemove(Connection conn, List<K> keys, long version);

    void batchRemove(Connection conn, Map<K, Long> keys);

}
