package springer.util;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Utility functions for reflection.
 *
 */
public final class ReflectionUtil {

    /**
     * Private constructor, because this is a utility class.
     */
    private ReflectionUtil() {
        // do nothing
    }

    /**
     * Class representing primitive types in Java.
     *
     */
    public static class Primitive {
        // These get initialized to their default values

        /** Default <tt>boolean</tt> value. */
        private static boolean defaultBoolean;

        /** Default <tt>byte</tt> value. */
        private static byte defaultByte;

        /** Default <tt>char</tt> value. */
        private static char defaultChar;

        /** Default <tt>short</tt> value. */
        private static short defaultShort;

        /** Default <tt>int</tt> value. */
        private static int defaultInt;

        /** Default <tt>long</tt> value. */
        private static long defaultLong;

        /** Default <tt>float</tt> value. */
        private static float defaultFloat;

        /** Default <tt>double</tt> value. */
        private static double defaultDouble;

        /**
         * Return corresponding default value for the class specified. Return <tt>null</tt> for non-primitive class.
         * @param  clazz primitive class
         * @return       boxed default value of the specified primitive class
         */
        public static Object getDefaultValue(final Class<?> clazz) {
            if (clazz.equals(boolean.class)) {
                return defaultBoolean;
            } else if (clazz.equals(byte.class)) {
                return defaultByte;
            } else if (clazz.equals(char.class)) {
                return defaultChar;
            } else if (clazz.equals(short.class)) {
                return defaultShort;
            } else if (clazz.equals(int.class)) {
                return defaultInt;
            } else if (clazz.equals(long.class)) {
                return defaultLong;
            } else if (clazz.equals(float.class)) {
                return defaultFloat;
            } else if (clazz.equals(double.class)) {
                return defaultDouble;
            } else {
                return null;
            }
        }

        /**
         * Get equivalent boxed class for specified primitive class.
         * @param  clazz primitive class
         * @return       boxed class
         */
        public static Class<?> toWrapper(final Class<?> clazz) {
            if (clazz.equals(boolean.class)) {
                return Boolean.class;
            } else if (clazz.equals(byte.class)) {
                return Byte.class;
            } else if (clazz.equals(char.class)) {
                return Character.class;
            } else if (clazz.equals(short.class)) {
                return Short.class;
            } else if (clazz.equals(int.class)) {
                return Integer.class;
            } else if (clazz.equals(long.class)) {
                return Long.class;
            } else if (clazz.equals(float.class)) {
                return Float.class;
            } else if (clazz.equals(double.class)) {
                return Double.class;
            } else {
                return clazz;
            }
        }

    }

    /**
     * Instantiate gracefully and return the instance.
     * @param  <T>                 type of the instance
     * @param  cls                 the class to instantiate
     * @param  args arguments      to pass to the constructor (in same order)
     * @param  shouldSetAccessible whether to setAccessible to true before instantiating (for non-public constructors)
     * @return                     instance of the class
     */
    public static <T> T instantiate(final Class<T> cls, final Object[] args, final boolean shouldSetAccessible) {
        if (args == null || args.length == 0) {
            try {
                return cls.newInstance();
            } catch (InstantiationException e) {
                throw new IllegalArgumentException(e);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }
        } else {
            @SuppressWarnings("unchecked")
            final Constructor<T>[] constructors = (Constructor<T>[]) cls.getConstructors();
            for (Constructor<T> each: constructors) {
                final Class<?>[] paramTypes = each.getParameterTypes();
                if (args.length == paramTypes.length) {
                    boolean match = true;
                    for (int i = 0; i < args.length; i++) {
                        // args[i] == null ..implies that.. match = true (for this particular argument)
                        if (args[i] != null && !(Primitive.toWrapper(paramTypes[i]).isInstance(args[i]))) {
                            match = false;
                            break;
                        }
                    }
                    if (match) {
                        if (shouldSetAccessible) {
                            each.setAccessible(true);
                        }
                        try {
                            return each.newInstance(args);
                        } catch (InstantiationException e) {
                            throw new IllegalArgumentException(e);
                        } catch (IllegalAccessException e) {
                            throw new IllegalArgumentException(e);
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException(e);
                        } catch (InvocationTargetException e) {
                            throw new IllegalArgumentException(e);
                        }
                    }
                }
            }
            throw new IllegalArgumentException(String.format(
                    "No matching constructor found for %s in args: %s", cls, Arrays.toString(args)));
        }
    }

    /**
     * Somehow instantiate the class, even though with null values in fields.
     * @param  <T> instance type
     * @param  cls class to instantiate
     * @return     instance of class <tt>cls</tt>
     */
    public static <T> T instantiate(final Class<T> cls) {
        try {
            return cls.newInstance();
        } catch (InstantiationException e) {
            // use brute-force way of instantiation
            @SuppressWarnings("unchecked")
            final Constructor<T> constr = (Constructor<T>) cls.getConstructors()[0];
            final List<Object> params = new ArrayList<Object>();
            try {
                for (Class<?> pType : constr.getParameterTypes()) {
                    params.add(pType.isPrimitive() ? Primitive.toWrapper(pType).newInstance() : null);
                }
                return constr.newInstance(params.toArray());
            } catch (IllegalAccessException e2) {
                throw new IllegalArgumentException(e2);
            } catch (InvocationTargetException e2) {
                throw new IllegalArgumentException(e2);
            } catch (InstantiationException e2) {
                throw new IllegalArgumentException(e2);
            }
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Populate specified instance using a map of field names to corresponding values.
     * @param  <T>         object type being populated
     * @param  instance    object to populate
     * @param  fieldValues map of field names to values
     * @return             populated instance (same as the <tt>instance</tt> argument)
     */
    public static <T> T populate(final T instance, final Map<String, ? extends Object> fieldValues) {
        final Field[] fields = instance.getClass().getDeclaredFields();
        try {
            for (Field each: fields) {
                final int modifiers = each.getModifiers();
                each.setAccessible(true);
                final String fname = each.getName();
                if (fieldValues.containsKey(fname) && !Modifier.isFinal(modifiers)) {
                    each.set(instance, fieldValues.get(fname));
                }
            }
            return instance;
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Instantiate a class using attribute values in a supplied map.
     * @param  <T>   type of class to instantiate
     * @param  map   map of attribute names and values
     * @param  clazz class to instantiate
     * @return       instantiated object
     */
    @SuppressWarnings("unchecked")
    public static <T> T fromMap(final Map<String, ?> map, final Class<T> clazz) {
        final Field[] fields = clazz.getDeclaredFields();
        try {
            final T result = populate(instantiate(clazz), map);
            for (Field each: fields) {
                final int modifiers = each.getModifiers();
                each.setAccessible(true);
                final String fname = each.getName();
                final Object value = map.get(fname);
                final Class<?> fclass = each.getType();
                if (!Modifier.isFinal(modifiers)) {
                    final Object fvalue = value instanceof Map<?, ?> && !Map.class.isAssignableFrom(fclass)
                            ? fromMap((Map<String, ?>) value, fclass)
                                    : value == null && fclass.isPrimitive()
                                    ? Primitive.getDefaultValue(fclass)
                                            : value;
                    each.set(Modifier.isStatic(modifiers) ? null : result, cast(fclass, fvalue));
                }
            }
            return result;
        } catch (IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    /**
     * Cast a given object to another given class.
     * @param  clazz class to cast as
     * @param  value object to cast
     * @return       same object, but cast as specified class
     */
    private static Object cast(final Class<?> clazz, final Object value) {
        if (value instanceof Number) {
            final Number n = (Number) value;
            if (clazz.equals(byte.class) || clazz.equals(Byte.class)) {
                return n.byteValue();
            } else if (clazz.equals(short.class) || clazz.equals(Short.class)) {
                return n.shortValue();
            } else if (clazz.equals(int.class) || clazz.equals(Integer.class)) {
                return n.intValue();
            } else if (clazz.equals(long.class) || clazz.equals(Long.class)) {
                return n.longValue();
            } else if (clazz.equals(float.class) || clazz.equals(Float.class)) {
                return n.floatValue();
            } else if (clazz.equals(double.class) || clazz.equals(Double.class)) {
                return n.doubleValue();
            } else if (clazz.equals(BigDecimal.class)) {
                return new BigDecimal("" + value);
            }
            return value;
        }
        return value;

    }

    /**
     * Return field values of a given object, restricting the scope to specified class.
     * @param  <T>               type of scope class
     * @param  object            any object
     * @param  clazz             class to restrict the scope to
     * @param  includeTransients whether to include transients in the result
     * @return                   map of attribute names and values
     */
    public static <T> Map<String, Object> getFieldValues(final Object object, final Class<T> clazz,
            final boolean includeTransients) {
        if (!clazz.isInstance(object)) {
            throw new IllegalArgumentException("Object " + object + " is not an instance of class " + clazz.getName());
        }
        final Field[] fields = clazz.getDeclaredFields();
        final Map<String, Object> data = new LinkedHashMap<String, Object>(fields.length);
        for (Field each: fields) {
            if (!includeTransients && Modifier.isTransient(each.getModifiers())) {
                continue;
            }
            each.setAccessible(true);
            Object val;
            try {
                val = each.get(object);
                data.put(each.getName(), val);
            } catch (IllegalAccessException e) {
                throw new IllegalArgumentException(e);
            }
        }
        return data;
    }

    /**
     * Return a map of non-transient attribute names and values for a given object.
     * @param  object any object
     * @return        map of attribute names and values
     */
    public static Map<String, Object> toMap(final Object object) {
        return toMap(object, object.getClass());
    }

    /**
     * Return a map of non-transient attribute names and values for a given object, scoped to only specified class.
     * @param  <T>    scope class type
     * @param  object any object
     * @param  clazz  class to restrict the scope to
     * @return        map of attribute names and values
     */
    public static <T> Map<String, Object> toMap(final Object object, final Class<T> clazz) {
        return toMap(object, clazz, false);
    }

    /** Regex for package names beginning with 'java' or 'javax'. */
    public static final String JDK_CLASS_REGEX = "(java|javax)\\..+";

    /**
     * Return a map of attribute names and values for a given object, restricting the scope to a specified class.
     * @param  <T>               scope class type
     * @param  object            any object
     * @param  clazz             class to restrict the scope to
     * @param  includeTransients whether to include transient fields in result
     * @return                   map of attribute names to values
     */
    public static <T> Map<String, Object> toMap(final Object object, final Class<T> clazz,
            final boolean includeTransients) {
        final Map<String, Object> fields = getFieldValues(object, clazz, includeTransients);
        final Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (Entry<String, Object> entry: fields.entrySet()) {
            final String key = entry.getKey();
            final Object value = entry.getValue();
            String cname = value == null ? null : value.getClass().getName();
            if (value == null || cname.matches(JDK_CLASS_REGEX) || value instanceof Map || value instanceof Collection) {
                if (value instanceof Map) {
                    result.put(key, normalize((Map<?, ?>) value));
                } else {
                    result.put(key, value);
                }
            } else {
                result.put(key, toMap(value));
            }
        }
        return result;
    }

    /**
     * In a map, expand value instances of non-JDK classes as maps and return the converted map.
     * @param  map map of attribute names to values
     * @return     converted map where instances of non-JDK classes are expanded to maps
     */
    public static Map<Object, Object> normalize(final Map<?, ?> map) {
        final Map<Object, Object> result = new LinkedHashMap<Object, Object>(map.size());
        for (Entry<?, ?> each: map.entrySet()) {
            final Object oldKey = each.getKey();
            final Class<?> oldKeyClass = oldKey.getClass();
            final Object newKey = oldKeyClass.getName().matches(JDK_CLASS_REGEX) ? oldKey : toMap(oldKey);
            final Object oldValue = each.getValue();
            Object newValue = null;
            if (oldValue != null) {
                final Class<?> oldValueClass = oldValue.getClass();
                newValue = oldValueClass.getName().matches(JDK_CLASS_REGEX) ? oldValue : toMap(oldValue);
            }
            result.put(newKey, newValue);
        }
        return result;
    }

}
