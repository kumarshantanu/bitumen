package springer.jdbc.kv;

import java.io.Serializable;

import springer.util.Util;

/**
 * Key, value and version tuple.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class KeyValueVersion<K, V> implements Serializable {

    /**
     * Class version; required by {@link Serializable}.
     */
    private static final long serialVersionUID = 1L;

    /**
     * The key.
     */
    private final K key;

    /**
     * The value.
     */
    private final V value;

    /**
     * The version.
     */
    private final Long version;

    /**
     * Getter for key.
     * @return key
     */
    public final K getKey() {
        return key;
    }

    /**
     * Getter for value.
     * @return value
     */
    public final V getValue() {
        return value;
    }

    /**
     * Getter for version.
     * @return version
     */
    public final Long getVersion() {
        return version;
    }

    /**
     * Construct instance from required arguments.
     * @param  theKey     the key
     * @param  theValue   the value
     * @param  theVersion the version
     */
    public KeyValueVersion(final K theKey, final V theValue, final Long theVersion) {
        this.key = theKey;
        this.value = theValue;
        this.version = theVersion;
    }

    @Override
    public final String toString() {
        return String.format("key=%s, value=%s, version=%d", key, value, version);
    }

    @Override
    public final int hashCode() {
        final String compositeString = "" + key + '|' + value + '|' + version;
        return compositeString.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null || !(obj instanceof KeyValueVersion)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        @SuppressWarnings("unchecked")
        KeyValueVersion<K, V> that = (KeyValueVersion<K, V>) obj;
        return Util.equals(key, that.key) && Util.equals(value, that.value) && Util.equals(version, that.version);
    }

}
