package springer.di;

import java.util.LinkedHashMap;
import java.util.Map;

public class DependencyBuilder<K> implements IDependencyBuilder<K> {

    private final Map<K, IComponentSource<?, K>> graph = new LinkedHashMap<K, IComponentSource<?, K>>();

    // ----- core builder methods -----

    public DependencyBuilder<K> set(K key, IComponentSource<?, K> source) {
        if (key == null) {
            throw new IllegalArgumentException("Key cannot be null");
        }
        if (graph.containsKey(key)) {
            throw new IllegalArgumentException("Key already defined: " + key);
        }
        graph.put(key, source);
        return this;
    }

    // ----- syntactic sugar -----

    public DependencyBuilder<K> add(K key, IComponentSource<?, K> getter) {
        if (graph.containsKey(key)) {
            throw new IllegalArgumentException("Key already defined: " + key);
        }
        return set(key, getter);
    }

    public DependencyBuilder<K> addSingleton(K key, IComponentSource<?, K> source) {
        if (source instanceof DI.ComponentSourceSingleton) {
            return add(key, source);
        }
        return add(key, DI.singleton(source));
    }

    public DependencyBuilder<K> addFactory(K key, IComponentSource<?, K> source) {
        if (source instanceof DI.ComponentSourceSingleton) {
            throw new IllegalArgumentException("Expected component-source to be a factory but found singleton");
        }
        return add(key, source);
    }

    public DependencyBuilder<K> addConstant(K key, Object value) {
        if (value instanceof IComponentSource) {
            throw new IllegalArgumentException("Value can not be instance of " + IComponentSource.class);
        }
        @SuppressWarnings("unchecked")
        final Class<K> keyClass = (Class<K>) key.getClass();
        return add(key, DI.constantly(value, keyClass));
    }

    // ----- get the map that was built -----

    public Map<K, IComponentSource<?, K>> getDependencyMap() {
        return graph;
    }

    // ----- obtaining instance -----

    public <T> T getInstance(K componentKey, Class<T> clazz) {
        return DI.getInstance(graph, componentKey, clazz);
    }

}
