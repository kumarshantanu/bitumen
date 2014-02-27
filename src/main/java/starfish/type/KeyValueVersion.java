package starfish.type;

import java.io.Serializable;

import starfish.helper.Util;

public class KeyValueVersion<K, V> implements Serializable {

    private static final long serialVersionUID = 1L;

    public final K key;
    public final V value;
    public final Long version;

    public KeyValueVersion(final K key, final V value, final Long version) {
        this.key = key;
        this.value = value;
        this.version = version;
    }

    @Override
    public String toString() {
        return String.format("key=%s, value=%v, version=%d", key, value, version);
    }

    @Override
    public int hashCode() {
        return ("" + key + '|' + value + '|' + version).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (!(obj instanceof KeyValueVersion)) return false;
        @SuppressWarnings("unchecked")
        KeyValueVersion<K, V> that = (KeyValueVersion<K, V>) obj;
        return Util.equals(key, that.key) &&
                Util.equals(value, that.value) &&
                Util.equals(version, that.version);
    }

}
