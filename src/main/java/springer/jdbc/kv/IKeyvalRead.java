package springer.jdbc.kv;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * Read operations for Key-value store.
 *
 * @param  <K> key type
 * @param  <V> value type
 */
public interface IKeyvalRead<K, V> {

    // ---- contains ----

    /**
     * Find specified key in the store and return the current version. Return <tt>null</tt> if not found.
     * @param  conn JDBC connection
     * @param  key  key to find
     * @return      current version of the value (<tt>null</tt> if not found)
     */
    Long contains(Connection conn, K key);

    /**
     * Find specified keys in the store and return the current versions. Return <tt>null</tt> for the keys not found.
     * @param  conn JDBC connection
     * @param  keys keys to find
     * @return      current versions of the values (<tt>null</tt> if not found)
     */
    List<Long> batchContains(Connection conn, List<K> keys);

    // ---- containsVersion (requires old version) ----

    /**
     * Return <tt>true</tt> if specified key has specified version in the store, <tt>false</tt> otherwise.
     * @param  conn    JDBC connection
     * @param  key     key to find
     * @param  version version to match
     * @return         <tt>true</tt> if specified key has specified version in the store, <tt>false</tt> otherwise
     */
    boolean containsVersion(Connection conn, K key, long version);

    /**
     * Match specified keys and versions in store and return <tt>true</tt> for each match, <tt>false</tt> otherwise.
     * @param  conn        JDBC connection
     * @param  keyVersions keys and versions to match
     * @return             map of keys to corresponding match result
     */
    Map<K, Boolean> batchContainsVersion(Connection conn, Map<K, Long> keyVersions);

    // ---- read ----

    /**
     * Find specified key in store and return corresponding value, <tt>null</tt> if key not found.
     * @param  conn JDBC connection
     * @param  key  key to find
     * @return      current value, <tt>null</tt> if key not found
     */
    V read(Connection conn, K key);

    /**
     * Find specified keys in store and return corresponding values, <tt>null</tt> if key not found.
     * @param  conn JDBC connection
     * @param  keys keys to find
     * @return      current values, <tt>null</tt> if key not found
     */
    Map<K, V> batchRead(Connection conn, List<K> keys);

    // ---- readVersion (requires old version) ----

    /**
     * Find specified key and version in store, if there's a match then return value else return <tt>null</tt>.
     * @param  conn    JDBC connection
     * @param  key     key to find
     * @param  version version to match
     * @return         current value if there's a key/version match, <tt>null</tt> otherwise
     */
    V readForVersion(Connection conn, K key, long version);

    /**
     * Find specified keys and versions in store, if there's a match then return value else return <tt>null</tt>.
     * @param  conn        JDBC connection
     * @param  keyVersions map of keys and versions
     * @return             current values when there's key/version match, <tt>null</tt> otherwise
     */
    Map<K, V> batchReadForVersion(Connection conn, Map<K, Long> keyVersions);

    // ---- readAll ----

    /**
     * Find specified key in store and return corresponding value and version tuple, else <tt>null</tt>.
     * @param  conn JDBC connection
     * @param  key  key to find
     * @return      current value and version tuple, or <tt>null</tt> if key not found
     */
    ValueVersion<V> readAll(Connection conn, K key);

    /**
     * Find specified keys in store and return corresponding value and version tuples, else <tt>null</tt>.
     * @param  conn JDBC connection
     * @param  keys keys to find
     * @return      current value and version tuples, or <tt>null</tt> if key not found
     */
    Map<K, ValueVersion<V>> batchReadAll(Connection conn, List<K> keys);

}
