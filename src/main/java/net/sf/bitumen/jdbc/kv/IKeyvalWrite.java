package net.sf.bitumen.jdbc.kv;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Write operations for key-value store. Version of the value is not incremental.
 *
 * @param  <K> key type
 * @param  <V> value type
 */
public interface IKeyvalWrite<K, V> {

    // ---- insert (fails if key already exists) ----

    /**
     * Insert key-value pair into the store. Throw {@link RuntimeException} if key already exists in the store.
     * @param  conn  JDBC connection
     * @param  key   key to insert
     * @param  value value to insert
     * @return       version of the inserted key-value pair
     */
    long insert(Connection conn, K key, V value);

    /**
     * Insert key-value pairs into the store. Throw {@link RuntimeException} if key already exists in the store.
     * @param  conn  JDBC connection
     * @param  pairs key-value pairs to insert
     * @return       version of the inserted key-value pairs
     */
    long batchInsert(Connection conn, Map<K, V> pairs);

    // ---- save, regardless of whether they already exist ----

    /**
     * Upsert (Update or Insert) the specified key-value pair and return version.
     * @param  conn  JDBC connection
     * @param  key   key to be saved
     * @param  value value to be saved
     * @return       version after saving
     */
    long save(Connection conn, K key, V value);

    /**
     * Upsert (Update or Insert) the specified key-value pairs and return version.
     * @param  conn  JDBC connection
     * @param  pairs key-value pairs to be saved
     * @return       version after saving
     */
    long batchSave(Connection conn, Map<K, V> pairs);

    // ---- swap (requires old version) ----

    /**
     * Update value only if specified key and version match in the store.
     * @param  conn    JDBC connection
     * @param  key     key to find
     * @param  value   value to update
     * @param  version version to match
     * @return         new version if update successful, old version if no match
     */
    Long swap(Connection conn, K key, V value, long version);

    /**
     * Update key-value pairs if specified key and version match in the store.
     * @param  conn    JDBC connection
     * @param  pairs   key-value pairs
     * @param  version version to match
     * @return         new version if update successful, old version if no match
     */
    Long batchSwap(Connection conn, Map<K, V> pairs, long version);

    /**
     * Update key-value pairs if specified key and version match in the store.
     * @param  conn     JDBC connection
     * @param  triplets key, value and version triplets
     * @return          new version if update successful, old version if no match
     */
    Long batchSwap(Connection conn, List<KeyValueVersion<K, V>> triplets);

    // ---- touch (update version) ----

    /**
     * Update version of specified key. Return new version on successful update, <tt>null</tt> if key not found.
     * @param  conn JDBC connection
     * @param  key  key to find
     * @return      new version on successful update, <tt>null</tt> if key not found
     */
    Long touch(Connection conn, K key);

    /**
     * Update version of specified keys. Return new version on successful update, <tt>null</tt> for keys not found.
     * @param  conn JDBC connection
     * @param  keys keys to find
     * @return      new version on successful update, <tt>null</tt> for keys not found
     */
    Long batchTouch(Connection conn, List<K> keys);

    // ---- delete ----

    /**
     * Unconditionally delete the specified key and associated data.
     * @param  conn JDBC connection
     * @param  key  key to find and delete
     */
    void delete(Connection conn, K key);

    /**
     * Unconditionally delete the specified key and associated data.
     * @param  conn JDBC connection
     * @param  keys keys to find and delete
     */
    void batchDelete(Connection conn, List<K> keys);

    // ---- remove (requires old version) ----

    /**
     * Delete the specified key and associated data if specified version matches current version.
     * @param  conn    JDBC connection
     * @param  key     key to find
     * @param  version version to match
     */
    void remove(Connection conn, K key, long version);

    /**
     * Delete the specified keys and associated data if specified version matches current version.
     * @param  conn    JDBC connection
     * @param  keys    keys to find
     * @param  version version to match
     */
    void batchRemove(Connection conn, List<K> keys, long version);

    /**
     * Delete the specified keys and associated data if corresponding version matches current version.
     * @param  conn JDBC connection
     * @param  keys keys to find
     */
    void batchRemove(Connection conn, Map<K, Long> keys);

}
