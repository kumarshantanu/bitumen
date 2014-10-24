package springer.di;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Default {@link IDependencyBuilder} implementation.
 *
 * @param  <K> component key type
 */
public class DependencyBuilder<K> implements IDependencyBuilder<K> {

    /**
     * Internal map to store component sources with embedded dependencies on other components in the same map.
     */
    private final Map<K, IComponentSource<?, K>> graph =
            Collections.synchronizedMap(new LinkedHashMap<K, IComponentSource<?, K>>());

    // ----- core builder methods -----

    @Override
    public final DependencyBuilder<K> set(final K key, final IComponentSource<?, K> source) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (source == null) {
            throw new IllegalArgumentException("Source cannot be null");
        }
        graph.put(key, source);
        return this;
    }

    // ----- syntactic sugar -----

    @Override
    public final DependencyBuilder<K> add(final K key, final IComponentSource<?, K> getter) {
        if (graph.containsKey(key)) {
            throw new IllegalArgumentException("Key already defined: " + key);
        }
        return set(key, getter);
    }

    @Override
    public final DependencyBuilder<K> addSingleton(final K key, final IComponentSource<?, K> source) {
        if (source instanceof DI.ComponentSourceSingleton) {
            return add(key, source);
        }
        return add(key, DI.singleton(source));
    }

    @Override
    public final DependencyBuilder<K> addFactory(final K key, final IComponentSource<?, K> source) {
        if (source instanceof DI.ComponentSourceSingleton) {
            throw new IllegalArgumentException("Expected component-source to be a factory but found singleton");
        }
        return add(key, source);
    }

    @Override
    public final DependencyBuilder<K> addConstant(final K key, final Object value) {
        if (value instanceof IComponentSource) {
            throw new IllegalArgumentException("Value can not be instance of " + IComponentSource.class);
        }
        @SuppressWarnings("unchecked")
        final Class<K> keyClass = (Class<K>) key.getClass();
        return add(key, DI.constantly(value, keyClass));
    }

    // ----- get the map that was built -----

    @Override
    public final Map<K, IComponentSource<?, K>> getDependencyMap() {
        return graph;
    }

    // ----- obtaining instance -----

    @Override
    public final <T> T getInstance(final K componentKey, final Class<T> clazz) {
        return DI.getInstance(graph, componentKey, clazz);
    }

}
