package springer.di;

import java.util.Map;

/**
 * Functional interface representing a component source or getter (could be factory, or singleton or proxy etc).
 *
 * @param <T> ComponentType
 * @param <K> ComponentKey
 */
public interface IComponentSource<T, K> {

    /**
     * Given dependencies (or a dependency graph), obtain component and return the same.
     * @param dependencies zero or more dependencies, or a dependency graph
     * @return             the component
     */
    T get(Map<K, IComponentSource<?, K>> dependencies);

}
