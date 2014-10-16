package springer.di;

import java.util.Map;

public interface IDependencyBuilder<K> {

    // ----- core builder methods -----

    public DependencyBuilder<K> set(K key, IComponentSource<?, K> source);

    // ----- syntactic sugar -----

    public DependencyBuilder<K> add(K key, IComponentSource<?, K> getter);

    public DependencyBuilder<K> addSingleton(K key, IComponentSource<?, K> source);

    public DependencyBuilder<K> addFactory(K key, IComponentSource<?, K> source);

    public DependencyBuilder<K> addConstant(K key, Object value);

    // ----- get the map that was built -----

    public Map<K, IComponentSource<?, K>> getDependencyMap();

    // ----- obtaining instance -----

    public <T> T getInstance(K componentKey, Class<T> clazz);

}
