package net.sf.bitumen.di;

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

import net.sf.bitumen.util.ReflectionUtil;
import net.sf.bitumen.util.ThreadUtil;
import net.sf.bitumen.util.Util;

/**
 * Utility class for Dependency Injection, entirely consisting of static methods and inner classes. Most of the real,
 * grunt work is done here.
 *
 */
public final class DI {

    /**
     * Private constructor because class need not be instantiated.
     */
    private DI() {
        // do nothing, this is an unusable constructor on purpose
    }

    /**
     * Find specified <tt>componentKey</tt> in dependency map, fetch corresponding component source and finally return
     * the component instance.
     * @param  <T>          type of the component
     * @param  <K>          type of the component key
     * @param  dependencies dependency map (graph)
     * @param  componentKey component key used to identify a component source
     * @param  clazz        class that the return value should be cast to
     * @return              component instance
     */
    public static <T, K> T getInstance(final Map<K, IComponentSource<?, K>> dependencies, final K componentKey,
            final Class<T> clazz) {
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
                    clazz, instance, Util.getClassName(instance)));
        }
        return clazz.cast(instance);
    }

    /**
     * Find specified <tt>componentKeys</tt> in dependency map, fetch corresponding component sources concurrently
     * using an auto created CPU-bound thread pool and finally return the component instances. All component instances
     * should be of the same type.
     * @param  <T>           type of the component
     * @param  <K>           type of the component key
     * @param  dependencies  dependency map (graph)
     * @param  componentKeys component key used to identify a component source
     * @param  clazz         class that all component instances should be cast to
     * @return               list of component instances
     */
    public static <T, K> List<T> getInstances(final Map<K, IComponentSource<?, K>> dependencies,
            final List<K> componentKeys, final Class<T> clazz) {
        // assume CPU-bound initialization, hence allocate thread pool size = CPU cores * 2 + 1
        final ExecutorService threadPool = Executors.newFixedThreadPool(
                Runtime.getRuntime().availableProcessors() * 2 + 1);
        try {
            return getInstances(dependencies, componentKeys, clazz, threadPool);
        } finally {
            threadPool.shutdownNow();
        }
    }

    /**
     * Find specified <tt>componentKeys</tt> in dependency map, fetch corresponding component sources concurrently
     * using specified thread pool and finally return the component instances. All component instances should be of the
     * same type.
     * @param  <T>           type of the component
     * @param  <K>           type of the component key
     * @param  dependencies  dependency map (graph)
     * @param  componentKeys component key used to identify a component source
     * @param  clazz         class that all component instances should be cast to
     * @param  threadPool    thread pool to concurrently fetch component sources
     * @return               list of component instances
     */
    public static <T, K> List<T> getInstances(final Map<K, IComponentSource<?, K>> dependencies,
            final List<K> componentKeys, final Class<T> clazz, final ExecutorService threadPool) {
        try {
            return getInstances(dependencies, componentKeys, clazz, threadPool, -1);
        } catch (TimeoutException e) {
            throw new IllegalStateException("Unexpected TimeoutException - internal error, please report this issue");
        }
    }

    /**
     * Find specified <tt>componentKeys</tt> in dependency map, fetch corresponding component sources concurrently
     * using specified thread pool and finally return the component instances. All component instances should be of the
     * same type. {@link TimeoutException} is thrown if specified <tt>timeoutMillis</tt> milliseconds or more is elapsed.
     * @param  <T>           type of the component
     * @param  <K>           type of the component key
     * @param  dependencies  dependency map (graph)
     * @param  componentKeys component key used to identify a component source
     * @param  clazz         class that all component instances should be cast to
     * @param  threadPool    thread pool to concurrently fetch component sources
     * @param  timeoutMillis timeout duration in milliseconds - if negative value then ignored, if >= 0 and duration is
     *                       elapsed then {@link TimeoutException} is thrown
     * @return               list of component instances
     * @throws TimeoutException when <tt>timeoutMillis</tt> is 0 or more, and the duration is elapsed
     */
    public static <T, K> List<T> getInstances(final Map<K, IComponentSource<?, K>> dependencies,
            final List<K> componentKeys, final Class<T> clazz, final ExecutorService threadPool,
            final long timeoutMillis) throws TimeoutException {
        final long sleepMillis = 100L;
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
                        ThreadUtil.sleep(Math.min(sleepMillis, endTS - now));
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

    /**
     * Component source that wraps a constant value and always returns the same value.
     *
     * @param <T> component type
     * @param <K> component key type
     */
    public static class ConstantSource<T, K> implements IComponentSource<T, K> {

        /**
         * Component (constant) that is cached.
         */
        private final T component;

        /**
         * Construct component source from specified component (constant).
         * @param  constant the component to be wrapped by this source
         */
        public ConstantSource(final T constant) {
            this.component = constant;
        }

        @Override
        public final T get(final Map<K, IComponentSource<?, K>> dependencies) {
            return component;
        }

    }

    /**
     * Create a component factory that constantly returns the specified component.
     * @param  <T>               component type
     * @param  <K>               component key type
     * @param  component         constant value that will be wrapped in a source
     * @param  componentKeyClass component key class, which is required because "type erasure"!
     * @return                   component source
     */
    public static <T, K> IComponentSource<T, K> constantly(final T component, final Class<K> componentKeyClass) {
        return new ConstantSource<T, K>(component);
    }

    /**
     * Singleton wrapper class for component sources. It is safe to use with both component factories and non-factories.
     *
     * @param  <T> component type
     * @param  <K> component key type
     */
    public static class ComponentSourceSingleton<T, K> implements IComponentSource<T, K> {

        /**
         * Original component factory (presumably a factory) responsible for generating the component.
         */
        private final IComponentSource<T, K> origComponentFactory;

        /**
         * Cached component, after being generated for the first time.
         */
        private volatile T component = null;

        /**
         * Flag to tell whether component has been generated and assigned to <tt>component</tt>.
         */
        private volatile boolean initialized = false;

        /**
         * A lock object that is used to avoid race condition when ascertaining initialization.
         */
        private final Object lock = new Object();

        /**
         * Construct singleton source from specified component factory.
         * @param  componentFactory component factory that generates the component
         */
        public ComponentSourceSingleton(final IComponentSource<T, K> componentFactory) {
            if (componentFactory == null) {
                throw new IllegalArgumentException("Component factory cannot be null");
            }
            this.origComponentFactory = componentFactory;
        }

        @Override
        public final T get(final Map<K, IComponentSource<?, K>> dependencies) {
            synchronized (lock) {
                if (!initialized) {
                    component = origComponentFactory.get(dependencies);
                    initialized = true;
                }
            }
            return component;
        }

    }

    /**
     * Given a component factory, create a singleton out of it so that the generated value can be cached and reused.
     * @param  <T>              component type
     * @param  <K>              component key type
     * @param  componentFactory component factory
     * @return                  singleton source of the value generated by factory
     */
    public static <T, K> IComponentSource<T, K> singleton(final IComponentSource<T, K> componentFactory) {
        return new ComponentSourceSingleton<T, K>(componentFactory);
    }

    /**
     * Given a class and its constructor dependencies identified by component keys in a dependency graph, create and
     * return a component source that (at runtime) searches for the specified component keys in the dependency graph
     * and constructs and returns the component.
     * @param  <T>     component type
     * @param  <K>     component key type
     * @param  clazz   class to instantiate
     * @param  argKeys dependency component keys required to instantiate the <tt>clazz</tt> class
     * @return         component source that instantiates <tt>clazz</tt> when <tt>get(..)</tt> is invoked
     */
    public static <T, K> IComponentSource<T, K> constructByKey(final Class<T> clazz,
            @SuppressWarnings("unchecked") final K...argKeys) {
        return instantiateByKey(clazz, false, argKeys);
    }

    /**
     * Component source that instantiates a class by looking up constructor dependencies by component key in a
     * dependency graph.
     *
     * @param  <T> component type
     * @param  <K> component key type
     */
    public static class NewInstanceComponentSourceByKey<T, K> implements IComponentSource<T, K> {

        /**
         * Class object that is to be instantiated later.
         */
        private final Class<T> clazz;

        /**
         * Dependency component keys that are looked up in a dependency graph.
         */
        private final K[] constructorArgKeys;

        /**
         * Flag to tell whether <tt>.setAccessible(true)</tt> should be invoked on constructors of <tt>clazz</tt>.
         */
        private final boolean shouldSetAccessibleConstructor;

        /**
         * Construct component source from specified class, its (constructor) dependency component keys and a flag to
         * tell whether <tt>.setAccessible(true)</tt> should be called on its matching constructors.
         * @param  classToInstantiate  class to instantiate
         * @param  argKeys             dependency component keys (constructor dependencies)
         * @param  shouldSetAccessible whether <tt>.setAccessible(true)</tt> should be called on matching constructors -
         *                             <tt>true</tt> allows invoking non-public constructors; specify <tt>false</tt> if
         *                             your JVM has a disallowing security manager
         */
        public NewInstanceComponentSourceByKey(final Class<T> classToInstantiate, final K[] argKeys,
                final boolean shouldSetAccessible) {
            this.clazz = classToInstantiate;
            this.constructorArgKeys = argKeys.clone();
            this.shouldSetAccessibleConstructor = shouldSetAccessible;
        }

        @Override
        public final T get(final Map<K, IComponentSource<?, K>> dependencies) {
            final Object[] args = new Object[constructorArgKeys.length];
            for (int i = 0; i < args.length; i++) {
                args[i] = getInstance(dependencies, constructorArgKeys[i], Object.class);
            }
            return ReflectionUtil.instantiate(clazz, args, shouldSetAccessibleConstructor);
        }
    }

    /**
     * Create and return a component source that can instantiate a specified class after looking up its constructor
     * dependencies by keys in a dependency graph.
     * @param  <T>                 component type
     * @param  <K>                 component key type
     * @param  clazz               class to instantiate
     * @param  shouldSetAccessible whether call <tt>.setAccessible(true)</tt> on matching constructors
     * @param  argKeys             constructor dependency keys for <tt>clazz</tt>
     * @return                     component source that instantiates class by looking up its constructor dependencies
     *                             by keys in a dependency graph
     */
    public static <T, K> IComponentSource<T, K> instantiateByKey(final Class<T> clazz,
            final boolean shouldSetAccessible, @SuppressWarnings("unchecked") final K...argKeys) {
        return new NewInstanceComponentSourceByKey<T, K>(clazz, argKeys, shouldSetAccessible);
    }

    /**
     * Create and return a component source that can instantiate a specified class using specified component sources
     * for its constructor dependencies.
     * @param  <T>        component type
     * @param  <K>        component key type
     * @param  clazz      class to instantiate
     * @param  argSources component sources to be used to obtain constructor arguments for <tt>clazz</tt>
     * @return            component source that can instantiate <tt>clazz</tt>
     */
    @SafeVarargs
    public static <T, K> IComponentSource<T, K> construct(final Class<T> clazz,
            final IComponentSource<?, K>...argSources) {
        return instantiate(clazz, false, argSources);
    }

    /**
     * Component source that can instantiate a class using constructor dependencies obtained from other component
     * sources.
     *
     * @param  <T> component type
     * @param  <K> component key type
     */
    public static class NewInstanceComponentSourceBySource<T, K> implements IComponentSource<T, K> {

        /**
         * Class to instantiate.
         */
        private final Class<T> clazz;

        /**
         * Component sources to be used to obtain constructor arguments (in same order) for <tt>clazz</tt>.
         */
        private final IComponentSource<?, K>[] constructorArgSources;

        /**
         * Flag to tell whether <tt>.setAccessible(true)</tt> should be called on the matching constructor.
         */
        private final boolean shouldSetAccessibleConstructor;

        /**
         * Construct component source from specified class to instantiate, component sources to obtain constructor
         * arguments and a flag telling whether to invoke <tt>.setAccessible(true)</tt> on matching constructor.
         * @param  classToInstantiate  class to instantiate
         * @param  argSources          component sources to obtain constructor arguments from
         * @param  shouldSetAccessible whether to invoke <tt>.setAccessible(true)</tt> on matching constructor
         */
        public NewInstanceComponentSourceBySource(final Class<T> classToInstantiate,
                final IComponentSource<?, K>[] argSources, final boolean shouldSetAccessible) {
            this.clazz = classToInstantiate;
            this.constructorArgSources = argSources.clone();
            this.shouldSetAccessibleConstructor = shouldSetAccessible;
        }

        @Override
        public final T get(final Map<K, IComponentSource<?, K>> dependencies) {
            final Object[] args = new Object[constructorArgSources.length];
            for (int i = 0; i < args.length; i++) {
                args[i] = constructorArgSources[i].get(dependencies);
            }
            return ReflectionUtil.instantiate(clazz, args, shouldSetAccessibleConstructor);
        }
    }

    /**
     * Create and return a component source that can instantiate a specified class using other component sources to
     * obtain the constructor arguments.
     * @param  <T>                 component type
     * @param  <K>                 component key type
     * @param  clazz               class to instantiate
     * @param  shouldSetAccessible whether invoke <tt>/setAccessible(true)</tt> on matching constructor
     * @param  argSources          component sources to obtain constructor arguments for <tt>clazz</tt> from
     * @return                     component source that can instantiate <tt>clazz</tt> and return component
     */
    public static <T, K> IComponentSource<T, K> instantiate(final Class<T> clazz, final boolean shouldSetAccessible,
            @SuppressWarnings("unchecked") final IComponentSource<?, K>...argSources) {
        return new NewInstanceComponentSourceBySource<T, K>(clazz, argSources, shouldSetAccessible);
    }

    /**
     * Given a component-key, this component source finds (at runtime) the corresponding source, then obtains the
     * component and returns it.
     *
     * @param <K> the key type
     */
    public static class KeyLookup<K> implements IComponentSource<Object, K> {

        /**
         * Component key to be looked up.
         */
        private final K componentKey;

        /**
         * Constructor component source from component key.
         * @param  key component key
         */
        public KeyLookup(final K key) {
            this.componentKey = key;
        }

        @Override
        public final Object get(final Map<K, IComponentSource<?, K>> dependencies) {
            final IComponentSource<?, K> source = dependencies.get(componentKey);
            if (source == null) {
                throw new IllegalArgumentException(String.format(
                        "Expected an %s instance, but found NULL", IComponentSource.class));
            }
            return source.get(dependencies);
        }
    }

    /**
     * Given a component key, create and return a component source that can lookup the key in a dependency graph and
     * obtain component from the corresponding component source.
     * @param  <K> component key type
     * @param  key component key
     * @return     component source that looks up <tt>key</tt> in specified dependency graph
     */
    public static <K> IComponentSource<?, K> sourceOf(final K key) {
        return new KeyLookup<K>(key);
    }

    /**
     * Given a type map (key => type) and a class to be instantiated, return a component source that (at runtime)
     * auto-detects the matching constructor by looking up the type map and reflectively instantiates the component.
     * Constructors with maximum number of args are looked up first, followed by lower number of args.
     *
     * @param  <T> type of the component
     * @param  <K> key type for components
     */
    public static class AutoConstructSource<T, K> implements IComponentSource<T, K> {

        /**
         * Map of component keys to corresponding component types.
         */
        private final Map<K, Class<?>> types;

        /**
         * Class to be instantiated.
         */
        private final Class<T> clazz;

        /**
         * Flag to tell whether to invoke <tt>.setAccessible(true)</tt> on matching constructors.
         */
        private final boolean shouldSetAccessibleConstructor;

        /**
         * Construct component source from required parameters.
         * @param  componentKeyTypeMap component key-type map
         * @param  classToInstantiate  class to be instantiated
         * @param  shouldSetAccessible whether to invoke <tt>.setAccessible(true)</tt> on matching constructors
         */
        public AutoConstructSource(final Map<K, Class<?>> componentKeyTypeMap, final Class<T> classToInstantiate,
                final boolean shouldSetAccessible) {
            this.types = componentKeyTypeMap;
            this.clazz = classToInstantiate;
            this.shouldSetAccessibleConstructor = shouldSetAccessible;
        }

        /**
         * A tuple class to hold boolean match and matched value.
         *
         */
        private static class MatchValue {

            /**
             * The singleton <tt>MatchValue</tt> instance, which represents no match happened.
             */
            public static final MatchValue NO_MATCH = new MatchValue();

            /**
             * Flag to tell whether a match happened.
             */
            private final boolean match;

            /**
             * Matched value.
             */
            private final Object value;

            /**
             * Private constructor, used to create <tt>NO_MATCH</tt>.
             */
            private MatchValue() {
                this.match = false;
                this.value = null;
            }

            /**
             * Construct tuple from matched value.
             * @param  matchedValue matched value
             */
            public MatchValue(final Object matchedValue) {
                this.match = true;
                this.value = matchedValue;
            }

        }

        @Override
        public final T get(final Map<K, IComponentSource<?, K>> dependencies) {
            final Constructor<?>[] constructors = clazz.getConstructors();
            Arrays.sort(constructors, new Comparator<Constructor<?>>() { // sort by param count in descending order
                @Override
                public int compare(final Constructor<?> o1, final Constructor<?> o2) {
                    return o2.getParameterTypes().length - o1.getParameterTypes().length;
                }
            });
            for (Constructor<?> eachConstructor: constructors) {
                if (shouldSetAccessibleConstructor) {
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
                        return ReflectionUtil.instantiate(clazz, paramValues, shouldSetAccessibleConstructor);
                    }
                }
            }
            throw new IllegalArgumentException(String.format("No matching constructor found for %s in types %s",
                    clazz, types));
        }

        /**
         * Given component dependency graph and a class, return a matching component as a {@code MatchValue} tuple.
         * @param  dependencies component dependency graph
         * @param  paramClass   constructor parameter class
         * @return              {@code MatchValue} tuple representing successful or failed match
         */
        private MatchValue getParamInstance(final Map<K, IComponentSource<?, K>> dependencies,
                final Class<?> paramClass) {
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
                return MatchValue.NO_MATCH;
            } else {
                return new MatchValue(getInstance(dependencies, matchKey, paramClass));
            }
        }
    }

    /**
     * Return a component source that automatically detects the matching constructor parameters of a class in a
     * component dependency graph and instantiates it. Constructors with maximum number of arguments are tried first,
     * followed by those with lower number of arguments.
     * @param  <T>                 component type
     * @param  <K>                 component key type
     * @param  types               map of component keys to corresponding component types
     * @param  classToInstantiate  class to instantiate
     * @param  shouldSetAccessible whether to invoke <tt>.setAccessible(true)</tt> on matching constructors
     * @return                     component source that auto-constructs <tt>clazz</tt>
     */
    public static <T, K> IComponentSource<T, K> autoConstruct(final Map<K, Class<?>> types,
            final Class<T> classToInstantiate, final boolean shouldSetAccessible) {
        return new AutoConstructSource<T, K>(types, classToInstantiate, shouldSetAccessible);
    }

    /**
     * Return a component source that automatically detects the matching constructor parameters of a class in a
     * component dependency graph and instantiates it. Constructors with maximum number of arguments are tried first,
     * followed by those with lower number of arguments.
     * @param  <T>                 component type
     * @param  <K>                 component key type
     * @param  types               map of component keys to corresponding component types
     * @param  classToInstantiate  class to instantiate
     * @return                     component source that auto-constructs <tt>clazz</tt>
     */
    public static <T, K> IComponentSource<T, K> autoConstruct(final Map<K, Class<?>> types,
            final Class<T> classToInstantiate) {
        return new AutoConstructSource<T, K>(types, classToInstantiate, false);
    }

}
