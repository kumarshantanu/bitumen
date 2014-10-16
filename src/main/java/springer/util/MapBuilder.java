package springer.util;

import java.util.HashMap;
import java.util.Map;

public class MapBuilder<K, V> {

    private final Map<K, V> container;

    public MapBuilder(Map<K, V> container) {
        this.container = container;
    }

    public MapBuilder() {
        this(new HashMap<K, V>());
    }

    public MapBuilder<K, V> set(K key, V value) {
        container.put(key, value);
        return this;
    }

    public MapBuilder<K, V> add(K key, V value) {
        if (container.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " already exists");
        }
        return set(key, value);
    }

    public Map<K, V> get() {
        return container;
    }

    // ---------- Map population ----------

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value) {
        container.put(key, value);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2) {
        container.put(key, value);
        container.put(key2, value2);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2, K key3, V value3) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2, K key3, V value3,
            K key4, V value4) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2, K key3, V value3,
            K key4, V value4, K key5, V value5) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2, K key3, V value3,
            K key4, V value4, K key5, V value5, K key6, V value6) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        container.put(key6, value6);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2, K key3, V value3,
            K key4, V value4, K key5, V value5, K key6, V value6, K key7, V value7) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        container.put(key6, value6);
        container.put(key7, value7);
        return container;
    }

    public static <K, V> Map<K, V> put(Map<K, V> container, K key, V value, K key2, V value2, K key3, V value3,
            K key4, V value4, K key5, V value5, K key6, V value6, K key7, V value7, K key8, V value8) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        container.put(key6, value6);
        container.put(key7, value7);
        container.put(key8, value8);
        return container;
    }

    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> putAll(Map<K, V> container, Object...kvs) {
        if (kvs.length % 2 != 0) {
            throw new IllegalArgumentException("Expected even number of key-value arguments but found " + kvs.length);
        }
        for (int i = 0; i < kvs.length; i++) {
            container.put((K) kvs[i], (V) kvs[i + 1]);
        }
        return container;
    }

    // ---------- HashMap ----------

    public static <K, V> HashMap<K, V> hashMap(K key, V value) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2, K key3, V value3) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2, K key3, V value3, K key4, V value4) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2, K key3, V value3, K key4, V value4,
            K key5, V value5) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2, K key3, V value3, K key4, V value4,
            K key5, V value5, K key6, V value6) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2, K key3, V value3, K key4, V value4,
            K key5, V value5, K key6, V value6, K key7, V value7) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6, key7, value7);
        return container;
    }

    public static <K, V> HashMap<K, V> hashMap(K key, V value, K key2, V value2, K key3, V value3, K key4, V value4,
            K key5, V value5, K key6, V value6, K key7, V value7, K key8, V value8) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6, key7, value7,
                key8, value8);
        return container;
    }

}
