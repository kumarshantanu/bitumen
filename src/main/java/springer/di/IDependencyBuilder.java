package springer.di;

import java.util.Map;

/**
 * Fluent interface for building a dependency graph of component sources.
 *
 * @param <K> the key type, which is typically a Java enum (or sometimes string)
 */
public interface IDependencyBuilder<K> {

    // ----- core builder methods -----

    /**
     * Update or add a pair of key and corresponding component source.
     * @param key the key
     * @param source the component source associated with the key
     * @return <tt>IDependencyBuilder</tt> instance (usually same instance on which this method is invoked)
     */
    public DependencyBuilder<K> set(K key, IComponentSource<?, K> source);

    // ----- syntactic sugar -----

    /**
     * Add a pair of key and associated component source. Throw <tt>IllegalArgumentException</tt> if key already exists
     * in the dependency graph.
     * @param key the key
     * @param source the component source associated with the key
     * @return <tt>IDependencyBuilder</tt> instance (usually same instance on which this method is invoked)
     */
    public DependencyBuilder<K> add(K key, IComponentSource<?, K> source);

    /**
     * Add a pair of key and associated component source as a singleton. The source is wrapped into a singleton if not
     * already one. Throw <tt>IllegalArgumentException</tt> if key already exists in the dependency graph.
     * @param key the key
     * @param source the component source associated with the key
     * @return <tt>IDependencyBuilder</tt> instance (usually same instance on which this method is invoked)
     */
    public DependencyBuilder<K> addSingleton(K key, IComponentSource<?, K> source);

    /**
     * Add a pair of key and associated component source as a factory. Throw <tt>IllegalArgumentException</tt> if key
     * already exists in the dependency graph, or if source is a singleton.
     * @param key the key
     * @param source the component source associated with the key
     * @return <tt>IDependencyBuilder</tt> instance (usually same instance on which this method is invoked)
     */
    public DependencyBuilder<K> addFactory(K key, IComponentSource<?, K> source);

    /**
     * Add a pair of key and associated value wrapped as a component source. Throw <tt>IllegalArgumentException</tt>
     * if key already exists in the dependency graph, or if value is a component-source.
     * @param key the key
     * @param value the constant value to be returned by the source
     * @return <tt>IDependencyBuilder</tt> instance (usually same instance on which this method is invoked)
     */
    public DependencyBuilder<K> addConstant(K key, Object value);

    // ----- get the map that was built -----

    /**
     * Return the dependency graph built so far.
     * @return dependency graph, of type <tt>Map&lt;K, IComponentSource&lt;?, K&gt;&gt;</tt>
     */
    public Map<K, IComponentSource<?, K>> getDependencyMap();

    // ----- obtaining instance -----

    /**
     * Given component key and expected component type, resolve component source and return the component using the
     * dependency graph.
     * @param componentKey the component key
     * @param clazz expected type - a <tt>class</tt> object
     * @return component, which is cast to the type specified via <tt>clazz</tt>
     */
    public <T> T getInstance(K componentKey, Class<T> clazz);

}
