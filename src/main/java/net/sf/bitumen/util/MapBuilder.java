package net.sf.bitumen.util;

import java.util.HashMap;
import java.util.Map;

/**
 * A utility class to build {@link Map} instances.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class MapBuilder<K, V> {

    /** Storage for key-value pairs while they are being added. */
    private final Map<K, V> container;

    /**
     * Construct from a map container.
     * @param mapContainer map container
     */
    public MapBuilder(final Map<K, V> mapContainer) {
        this.container = mapContainer;
    }

    /**
     * Construct from no argument. Use a {@link HashMap} container.
     */
    public MapBuilder() {
        this(new HashMap<K, V>());
    }

    /**
     * Like {@link Map#put(Object, Object)}, but returns the {@link MapBuilder} instance.
     * @param  key   key to put
     * @param  value value to put
     * @return       {@link MapBuilder} instance
     */
    public final MapBuilder<K, V> set(final K key, final V value) {
        container.put(key, value);
        return this;
    }

    /**
     * Add key and value to the map. Throw {@link IllegalArgumentException} if key already exists.
     * @param  key   key to add
     * @param  value value to add
     * @return       {@link MapBuilder} instance
     */
    public final MapBuilder<K, V> add(final K key, final V value) {
        if (container.containsKey(key)) {
            throw new IllegalArgumentException("Key " + key + " already exists");
        }
        return set(key, value);
    }

    /**
     * Return the map built so far.
     * @return map built so far
     */
    public final Map<K, V> get() {
        return container;
    }

    // ---------- Map population ----------

    /**
     * Put 01 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value) {
        container.put(key, value);
        return container;
    }

    /**
     * Put 02 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2) {
        container.put(key, value);
        container.put(key2, value2);
        return container;
    }

    /**
     * Put 03 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @param  key3      key3 to put
     * @param  value3    value3 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2, final K key3, final V value3) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        return container;
    }

    /**
     * Put 04 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @param  key3      key3 to put
     * @param  value3    value3 to put
     * @param  key4      key4 to put
     * @param  value4    value4 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2, final K key3, final V value3, final K key4, final V value4) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        return container;
    }

    /**
     * Put 05 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @param  key3      key3 to put
     * @param  value3    value3 to put
     * @param  key4      key4 to put
     * @param  value4    value4 to put
     * @param  key5      key5 to put
     * @param  value5    value5 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2, final K key3, final V value3, final K key4, final V value4,
            final K key5, final V value5) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        return container;
    }

    /**
     * Put 06 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @param  key3      key3 to put
     * @param  value3    value3 to put
     * @param  key4      key4 to put
     * @param  value4    value4 to put
     * @param  key5      key5 to put
     * @param  value5    value5 to put
     * @param  key6      key6 to put
     * @param  value6    value6 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2, final K key3, final V value3, final K key4, final V value4,
            final K key5, final V value5, final K key6, final V value6) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        container.put(key6, value6);
        return container;
    }

    /**
     * Put 07 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @param  key3      key3 to put
     * @param  value3    value3 to put
     * @param  key4      key4 to put
     * @param  value4    value4 to put
     * @param  key5      key5 to put
     * @param  value5    value5 to put
     * @param  key6      key6 to put
     * @param  value6    value6 to put
     * @param  key7      key7 to put
     * @param  value7    value7 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2, final K key3, final V value3, final K key4, final V value4,
            final K key5, final V value5, final K key6, final V value6, final K key7, final V value7) {
        container.put(key, value);
        container.put(key2, value2);
        container.put(key3, value3);
        container.put(key4, value4);
        container.put(key5, value5);
        container.put(key6, value6);
        container.put(key7, value7);
        return container;
    }

    /**
     * Put 08 key-value pair to the specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  key       key to put
     * @param  value     value to put
     * @param  key2      key2 to put
     * @param  value2    value2 to put
     * @param  key3      key3 to put
     * @param  value3    value3 to put
     * @param  key4      key4 to put
     * @param  value4    value4 to put
     * @param  key5      key5 to put
     * @param  value5    value5 to put
     * @param  key6      key6 to put
     * @param  value6    value6 to put
     * @param  key7      key7 to put
     * @param  value7    value7 to put
     * @param  key8      key8 to put
     * @param  value8    value8 to put
     * @return           same map container where pair is put
     */
    public static <K, V> Map<K, V> put(final Map<K, V> container, final K key, final V value,
            final K key2, final V value2, final K key3, final V value3, final K key4, final V value4,
            final K key5, final V value5, final K key6, final V value6, final K key7, final V value7,
            final K key8, final V value8) {
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

    /**
     * Put arbitrary number of key-value pairs into a specified map.
     * @param  <K>       key type
     * @param  <V>       value type
     * @param  container map container
     * @param  kvs       key-value pairs, the count must be even numbered
     * @return           same map container where pair is put
     */
    @SuppressWarnings("unchecked")
    public static <K, V> Map<K, V> putAll(final Map<K, V> container, final Object...kvs) {
        if (kvs.length % 2 != 0) {
            throw new IllegalArgumentException("Expected even number of key-value arguments but found " + kvs.length);
        }
        for (int i = 0; i < kvs.length; i += 2) {
            container.put((K) kvs[i], (V) kvs[i + 1]);
        }
        return container;
    }

    // ---------- HashMap ----------

    /**
     * Return a {@link HashMap} of 01 key-value pairs.
     * @param  <K>   key type
     * @param  <V>   value type
     * @param  key   the key
     * @param  value the value
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value);
        return container;
    }

    /**
     * Return a {@link HashMap} of 02 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2);
        return container;
    }

    /**
     * Return a {@link HashMap} of 03 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @param  key3   the key3
     * @param  value3 the value3
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2,
            final K key3, final V value3) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3);
        return container;
    }

    /**
     * Return a {@link HashMap} of 04 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @param  key3   the key3
     * @param  value3 the value3
     * @param  key4   the key4
     * @param  value4 the value4
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2,
            final K key3, final V value3, final K key4, final V value4) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4);
        return container;
    }

    /**
     * Return a {@link HashMap} of 05 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @param  key3   the key3
     * @param  value3 the value3
     * @param  key4   the key4
     * @param  value4 the value4
     * @param  key5   the key5
     * @param  value5 the value5
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2,
            final K key3, final V value3, final K key4, final V value4, final K key5, final V value5) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5);
        return container;
    }

    /**
     * Return a {@link HashMap} of 06 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @param  key3   the key3
     * @param  value3 the value3
     * @param  key4   the key4
     * @param  value4 the value4
     * @param  key5   the key5
     * @param  value5 the value5
     * @param  key6   the key6
     * @param  value6 the value6
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2,
            final K key3, final V value3, final K key4, final V value4, final K key5, final V value5,
            final K key6, final V value6) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6);
        return container;
    }

    /**
     * Return a {@link HashMap} of 07 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @param  key3   the key3
     * @param  value3 the value3
     * @param  key4   the key4
     * @param  value4 the value4
     * @param  key5   the key5
     * @param  value5 the value5
     * @param  key6   the key6
     * @param  value6 the value6
     * @param  key7   the key7
     * @param  value7 the value7
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2,
            final K key3, final V value3, final K key4, final V value4, final K key5, final V value5,
            final K key6, final V value6, final K key7, final V value7) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6, key7, value7);
        return container;
    }

    /**
     * Return a {@link HashMap} of 08 key-value pairs.
     * @param  <K>    key type
     * @param  <V>    value type
     * @param  key    the key
     * @param  value  the value
     * @param  key2   the key2
     * @param  value2 the value2
     * @param  key3   the key3
     * @param  value3 the value3
     * @param  key4   the key4
     * @param  value4 the value4
     * @param  key5   the key5
     * @param  value5 the value5
     * @param  key6   the key6
     * @param  value6 the value6
     * @param  key7   the key7
     * @param  value7 the value7
     * @param  key8   the key8
     * @param  value8 the value8
     * @return       {@link HashMap} instance containing specified key-value pairs
     */
    public static <K, V> HashMap<K, V> hashMap(final K key, final V value, final K key2, final V value2,
            final K key3, final V value3, final K key4, final V value4, final K key5, final V value5,
            final K key6, final V value6, final K key7, final V value7, final K key8, final V value8) {
        final HashMap<K, V> container = new HashMap<K, V>();
        put(container, key, value, key2, value2, key3, value3, key4, value4, key5, value5, key6, value6, key7, value7,
                key8, value8);
        return container;
    }

}
