package springer.di;

import java.util.Map;

/**
 * Utility class to easily create component source from a key-value map.
 *
 * @param <K> component key type in dependency graph
 */
public class PropertyGetter<K> {

    /**
     * The key-value map to pick up properties from.
     */
    private final Map<?, ?> properties;

    /**
     * Component key class, required because "type erasure"!
     */
    private final Class<K> keyClass;

    /**
     * Construct a {@link PropertyGetter} instance from a map of property key-value pairs and component key type.
     * @param  keyValueMap       a map of key-value pairs
     * @param  componentKeyClass component type
     */
    public PropertyGetter(final Map<?, ?> keyValueMap, final Class<K> componentKeyClass) {
        this.properties = keyValueMap;
        this.keyClass = componentKeyClass;
    }

    /**
     * Given a property key, retrieve the corresponding value and return a component source wrapper of the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value
     */
    public final IComponentSource<?, K> get(final Object key) {
        return DI.constantly(properties.get(key), keyClass);
    }

    /**
     * Given a property key, retrieve the corresponding {@link String} value and return a component source wrapper of
     * the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value as {@link String}
     */
    public final IComponentSource<String, K> getString(final Object key) {
        final Object value = properties.get(key);
        if (value instanceof String || value == null) {
            return DI.constantly((String) value, keyClass);
        }
        return DI.constantly(value.toString(), keyClass);
    }

    /**
     * Given a property key, retrieve the corresponding {@link Boolean} value and return a component source wrapper of
     * the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value as {@link Boolean}
     */
    public final IComponentSource<Boolean, K> getBoolean(final Object key) {
        final Object value = properties.get(key);
        if (value instanceof Boolean) {
            return DI.constantly((Boolean) value, keyClass);
        }
        if (value instanceof String) {
            return DI.constantly(Boolean.parseBoolean((String) value), keyClass);
        }
        throw new IllegalArgumentException(String.format(
                "Corresponding value for key '%s' can not be parsed as Boolean: %s", key, value));
    }

    /**
     * Given a property key, retrieve the corresponding {@link Integer} value and return a component source wrapper of
     * the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value as {@link Integer}
     */
    public final IComponentSource<Integer, K> getInteger(final Object key) {
        final Object value = properties.get(key);
        if (value instanceof Number) {
            return DI.constantly(((Number) value).intValue(), keyClass);
        }
        if (value instanceof String) {
            return DI.constantly(Integer.parseInt((String) value), keyClass);
        }
        throw new IllegalArgumentException(String.format(
                "Corresponding value for key '%s' can not be parsed as Integer: %s", key, value));
    }

    /**
     * Given a property key, retrieve the corresponding {@link Long} value and return a component source wrapper of
     * the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value as {@link Long}
     */
    public final IComponentSource<Long, K> getLong(final Object key) {
        final Object value = properties.get(key);
        if (value instanceof Number) {
            return DI.constantly(((Number) value).longValue(), keyClass);
        }
        if (value instanceof String) {
            return DI.constantly(Long.parseLong((String) value), keyClass);
        }
        throw new IllegalArgumentException(String.format(
                "Corresponding value for key '%s' can not be parsed as Long: %s", key, value));
    }

    /**
     * Given a property key, retrieve the corresponding {@link Float} value and return a component source wrapper of
     * the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value as {@link Float}
     */
    public final IComponentSource<Float, K> getFloat(final Object key) {
        final Object value = properties.get(key);
        if (value instanceof Number) {
            return DI.constantly(((Number) properties.get(key)).floatValue(), keyClass);
        }
        if (value instanceof String) {
            return DI.constantly(Float.parseFloat((String) value), keyClass);
        }
        throw new IllegalArgumentException(String.format(
                "Corresponding value for key '%s' can not be parsed as Float: %s", key, value));
    }

    /**
     * Given a property key, retrieve the corresponding {@link Double} value and return a component source wrapper of
     * the value.
     * @param  key property key
     * @return     component source that always returns the corresponding property value as {@link Double}
     */
    public final IComponentSource<Double, K> getDouble(final Object key) {
        final Object value = properties.get(key);
        if (value instanceof Number) {
            return DI.constantly(((Number) value).doubleValue(), keyClass);
        }
        if (value instanceof String) {
            return DI.constantly(Double.parseDouble((String) value), keyClass);
        }
        throw new IllegalArgumentException(String.format(
                "Corresponding value for key '%s' can not be parsed as Double: %s", key, value));
    }

}
