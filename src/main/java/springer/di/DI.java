package springer.di;

import java.lang.reflect.Constructor;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import springer.util.ReflectionUtil;
import springer.util.ThreadUtil;

public class DI {

    public static <T, K> T getInstance(Map<K, IComponentSource<?, K>> dependencies, K componentKey, Class<T> clazz) {
        if (!dependencies.containsKey(componentKey)) {
            throw new IllegalArgumentException("No such component key: " + componentKey);
        }
        final IComponentSource<?, K> rawSource = dependencies.get(componentKey);
        if (rawSource == null) {
            throw new IllegalArgumentException(String.format("Component key %s points to null", componentKey));
        }
        final Object instance = rawSource.get(dependencies);
        if (instance != null && !clazz.isInstance(instance)) {
            throw new IllegalArgumentException(String.format(
                    "Expected instance of type %s, but found '%s' of type %s",
                            clazz, instance, instance==null? "NULL": instance.getClass().toString()));
        }
        return clazz.cast(instance);
    }

    public static <T, K> List<T> getInstances(final Map<K, IComponentSource<?, K>> dependencies, List<K> componentKeys,
            final Class<T> clazz) {
        // assume CPU-bound initialization, hence allocate thread pool size = CPU cores * 2 + 1
        final ExecutorService threadPool = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2 + 1);
        try {
            return getInstances(dependencies, componentKeys, clazz, threadPool);
        } finally {
            threadPool.shutdownNow();
        }
    }

    public static <T, K> List<T> getInstances(final Map<K, IComponentSource<?, K>> dependencies, List<K> componentKeys,
            final Class<T> clazz, ExecutorService threadPool) {
        try {
            return getInstances(dependencies, componentKeys, clazz, threadPool, -1);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Unexpected TimeoutException - internal error, please report this issue");
        }
    }

    public static <T, K> List<T> getInstances(final Map<K, IComponentSource<?, K>> dependencies, List<K> componentKeys,
            final Class<T> clazz, ExecutorService threadPool, long timeoutMillis) throws TimeoutException {
        final long SLEEP_MILLIS = 100L;
        final List<Future<T>> intermediateResult = new ArrayList<Future<T>>(componentKeys.size());
        for (final K each: componentKeys) {
            final Future<T> futureResult = threadPool.submit(new Callable<T>() {
                @Override
                public T call() throws Exception {
                    return getInstance(dependencies, each, clazz);
                }
            });
            intermediateResult.add(futureResult);
        }
        if (timeoutMillis > 0) {
            // wait for completion OR timeout - whichever happens earlier
            final long beginTS = System.currentTimeMillis();
            final long endTS = beginTS + timeoutMillis;
            boolean done = false;  // `done` implies Future<T>.isDone() = true for all submitted tasks
            while (!done) {
                done = true;
                for (Future<T> each: intermediateResult) {
                    if (!each.isDone()) {
                        done = false;
                        break; // proceed to timeout check
                    }
                }
                if (!done) {
                    final long now = System.currentTimeMillis();
                    if (now < endTS) {
                        ThreadUtil.sleep(Math.min(SLEEP_MILLIS, endTS - now));
                    } else {
                        throw new TimeoutException("Instances could not be obtained in " + timeoutMillis + "ms");
                    }
                }
            }
        }
        // collect result
        final List<T> result = new ArrayList<T>(componentKeys.size());
        for (Future<T> each: intermediateResult) {
            try {
                result.add(each.get());  // the each.get() call may block if timeout was not specified
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                throw new RuntimeException(e.getCause());
            }
        }
        return result;
    }

    public static class ConstantSource<T, K> implements IComponentSource<T, K> {
        private final T component;

        public ConstantSource(T component) {
            this.component = component;
        }

        @Override
        public T get(Map<K, IComponentSource<?, K>> dependencies) {
            return component;
        }
    }

    /**
     * Create a component factory that constantly returns the specified component.
     * @param component
     * @param componentKeyClass
     * @return component factory instance
     */
    public static <T, K> IComponentSource<T, K> constantly(final T component, Class<K> componentKeyClass) {
        return new ConstantSource<T, K>(component);
    }

    public static class ComponentSourceSingleton<T, K> implements IComponentSource<T, K> {
        private final IComponentSource<T, K> componentFactory;
        private volatile T component = null;
        private volatile boolean initialized = false;
        private final Object LOCK = new Object();

        public ComponentSourceSingleton(IComponentSource<T, K> componentFactory) {
            if (componentFactory == null) {
                throw new IllegalArgumentException("Component factory cannot be null");
            }
            this.componentFactory = componentFactory;
        }

        @Override
        public T get(Map<K, IComponentSource<?, K>> dependencies) {
            synchronized (LOCK) {
                if (!initialized) {
                    component = componentFactory.get(dependencies);
                    initialized = true;
                }
            }
            return component;
        }
    }

    /**
     * Given a component factory, create a singleton out of it so that the generated value can be reused.
     * @param componentFactory component factory
     * @return singleton instance of the value generated by factory
     */
    public static <T, K> IComponentSource<T, K> singleton(final IComponentSource<T, K> componentFactory) {
        return new ComponentSourceSingleton<T, K>(componentFactory);
    }

    public static <T, K> IComponentSource<T, K> constructByKey(final Class<T> clazz,
            final @SuppressWarnings("unchecked") K...argKeys) {
        return instantiateByKey(clazz, false, argKeys);
    }

    public static class NewInstanceComponentSourceByKey<T, K> implements IComponentSource<T, K> {
        private final Class<T> clazz;
        private final K[] argKeys;
        private final boolean shouldSetAccessible;

        public NewInstanceComponentSourceByKey(Class<T> clazz, K[] argKeys, boolean shouldSetAccessible) {
            this.clazz = clazz;
            this.argKeys = argKeys;
            this.shouldSetAccessible = shouldSetAccessible;
        }

        @Override
        public T get(Map<K, IComponentSource<?, K>> dependencies) {
            final Object[] args = new Object[argKeys.length];
            for (int i = 0; i < args.length; i++) {
                args[i] = getInstance(dependencies, argKeys[i], Object.class);
            }
            return ReflectionUtil.instantiate(clazz, args, shouldSetAccessible);
        }
    }

    public static <T, K> IComponentSource<T, K> instantiateByKey(final Class<T> clazz,
            final boolean shouldSetAccessible, final @SuppressWarnings("unchecked") K...argKeys) {
        return new NewInstanceComponentSourceByKey<T, K>(clazz, argKeys, shouldSetAccessible);
    }

    public static <T, K> IComponentSource<T, K> construct(final Class<T> clazz,
            final @SuppressWarnings("unchecked") IComponentSource<?, K>...argSources) {
        return instantiate(clazz, false, argSources);
    }

    public static class NewInstanceComponentSourceBySource<T, K> implements IComponentSource<T, K> {
        private final Class<T> clazz;
        private final IComponentSource<?, K>[] argSources;
        private final boolean shouldSetAccessible;

        public NewInstanceComponentSourceBySource(Class<T> clazz, IComponentSource<?, K>[] argSources,
                boolean shouldSetAccessible) {
            this.clazz = clazz;
            this.argSources = argSources;
            this.shouldSetAccessible = shouldSetAccessible;
        }

        @Override
        public T get(Map<K, IComponentSource<?, K>> dependencies) {
            final Object[] args = new Object[argSources.length];
            for (int i = 0; i < args.length; i++) {
                args[i] = argSources[i].get(dependencies);
            }
            return ReflectionUtil.instantiate(clazz, args, shouldSetAccessible);
        }
    }

    public static <T, K> IComponentSource<T, K> instantiate(final Class<T> clazz, final boolean shouldSetAccessible,
            final @SuppressWarnings("unchecked") IComponentSource<?, K>...argSources) {
        return new NewInstanceComponentSourceBySource<T, K>(clazz, argSources, shouldSetAccessible);
    }

    /**
     * Given a component-key, this component source finds (at runtime) the corresponding source, then obtains the
     * component and returns it.
     *
     * @param <K> the key type
     */
    public static class KeyLookup<K> implements IComponentSource<Object, K> {
        private final K key;

        public KeyLookup(K key) {
            this.key = key;
        }

        @Override
        public Object get(Map<K, IComponentSource<?, K>> dependencies) {
            final IComponentSource<?, K> source = dependencies.get(key);
            if (!(source instanceof IComponentSource)) {
                throw new IllegalArgumentException(String.format(
                        "Expected an %s instance, but found %s", IComponentSource.class, source));
            }
            return source.get(dependencies);
        }
    }

    public static <K> IComponentSource<?, K> sourceOf(K key) {
        return new KeyLookup<K>(key);
    }

    /**
     * Given a type map (key => type) and a class to be instantiated, return a component source that (at runtime)
     * auto-detects the matching constructor by looking up the type map and reflectively instantiates the component.
     * Constructors with maximum number of args are looked up first, followed by lower number of args.
     *
     * @param <T> type of the component
     * @param <K> key type for components
     */
    public static class AutoConstructSource<T, K> implements IComponentSource<T, K> {
        private final Map<K, Class<?>> types;
        private final Class<T> clazz;
        private final boolean shouldSetAccessible;

        public AutoConstructSource(Map<K, Class<?>> types, Class<T> clazz, boolean shouldSetAccessible) {
            this.types = types;
            this.clazz = clazz;
            this.shouldSetAccessible = shouldSetAccessible;
        }

        private class MatchValue {
            public final boolean match;
            public final Object value;
            public MatchValue(boolean match, Object value) {
                this.match = match;
                this.value = value;
            }
        }

        @Override
        public T get(Map<K, IComponentSource<?, K>> dependencies) {
            final Constructor<?>[] constructors = clazz.getConstructors();
            Arrays.sort(constructors, new Comparator<Constructor<?>>() { // sort by param count in descending order
                @Override
                public int compare(Constructor<?> o1, Constructor<?> o2) {
                    return o2.getParameterTypes().length - o1.getParameterTypes().length;
                }
            });
            for (Constructor<?> eachConstructor: constructors) {
                if (shouldSetAccessible) {
                    eachConstructor.setAccessible(true);
                }
                if (Modifier.isPublic(eachConstructor.getModifiers()) || eachConstructor.isAccessible()) {
                    boolean constructorMatch = true;
                    final Class<?>[] paramTypes = eachConstructor.getParameterTypes();
                    final Object[] paramValues = new Object[paramTypes.length];
                    for (int i = 0; i < paramTypes.length; i++) {
                        final MatchValue matchValue = getParamInstance(dependencies, ReflectionUtil.Primitive.toWrapper(
                                paramTypes[i]));
                        if (!matchValue.match) {
                            constructorMatch = false;
                            break;
                        }
                        paramValues[i] = matchValue.value;
                    }
                    if (constructorMatch) {
                        return ReflectionUtil.instantiate(clazz, paramValues, shouldSetAccessible);
                    }
                }
            }
            throw new IllegalArgumentException(String.format("No matching constructor found for %s in types %s",
                    clazz, types));
        }

        private MatchValue getParamInstance(Map<K, IComponentSource<?, K>> dependencies, Class<?> paramClass) {
            boolean classMatch = false;
            K matchKey = null;
            for (Entry<K, Class<?>> typePair: types.entrySet()) {
                final Class<?> typeClass = typePair.getValue();
                if (typeClass != null && paramClass.isAssignableFrom(typeClass)) { // ignore if typeClass==null
                    if (classMatch) { // classMatch already true - implies duplicate match!
                        throw new IllegalArgumentException(String.format(
                                "Expected exactly one match but found more than one match for %s: keys %s and %s",
                                paramClass, matchKey, typePair.getKey()));
                    }
                    classMatch = true;
                    matchKey = typePair.getKey();
                    // we continue the loop so that potential duplicate matches can be detected
                }
            }
            if (!classMatch) {
                return new MatchValue(false, null);
            } else {
                return new MatchValue(true, getInstance(dependencies, matchKey, paramClass));
            }
        }
    }

    public static <T, K> IComponentSource<T, K> autoConstruct(Map<K, Class<?>> types, Class<T> clazz,
            boolean shouldSetAccessible) {
        return new AutoConstructSource<T, K>(types, clazz, shouldSetAccessible);
    }

    public static <T, K> IComponentSource<T, K> autoConstruct(Map<K, Class<?>> types, Class<T> clazz) {
        return new AutoConstructSource<T, K>(types, clazz, false);
    }

}
