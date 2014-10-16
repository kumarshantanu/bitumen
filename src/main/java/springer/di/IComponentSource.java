package springer.di;

import java.util.Map;

/**
 * Functional interface representing a component source or getter (could be factory, or singleton or proxy etc).
 *
 * @param <T> ComponentType
 * @param <K> ComponentKey
 */
public interface IComponentSource<T, K> {

    public T get(Map<K, IComponentSource<?, K>> dependencies);

}
