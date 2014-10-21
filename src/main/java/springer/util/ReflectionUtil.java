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

public class ReflectionUtil {

    public static class Primitive {
        // These get initialized to their default values
        private static boolean DEFAULT_BOOLEAN;
        private static byte DEFAULT_BYTE;
        private static char DEFAULT_CHAR;
        private static short DEFAULT_SHORT;
        private static int DEFAULT_INT;
        private static long DEFAULT_LONG;
        private static float DEFAULT_FLOAT;
        private static double DEFAULT_DOUBLE;

        public static Object getDefaultValue(Class<?> clazz) {
            if (clazz.equals(boolean.class)) {
                return DEFAULT_BOOLEAN;
            } else if (clazz.equals(byte.class)) {
                return DEFAULT_BYTE;
            } else if (clazz.equals(char.class)) {
                return DEFAULT_CHAR;
            } else if (clazz.equals(short.class)) {
                return DEFAULT_SHORT;
            } else if (clazz.equals(int.class)) {
                return DEFAULT_INT;
            } else if (clazz.equals(long.class)) {
                return DEFAULT_LONG;
            } else if (clazz.equals(float.class)) {
                return DEFAULT_FLOAT;
            } else if (clazz.equals(double.class)) {
                return DEFAULT_DOUBLE;
            } else {
                return null;
            }
        }

        public static Class<?> toWrapper(Class<?> clazz) {
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
     * @param cls the class to instantiate
     * @param args arguments to pass to the constructor (in same order)
     * @param shouldSetAccessible whether to setAccessible to true before instantiating (for non-public constructors)
     * @return instance of the class
     */
    public static <T> T instantiate(Class<T> cls, Object[] args, boolean shouldSetAccessible) {
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
     * @param  cls class to instantiate
     * @return     instance of class <tt>cls</tt>
     */
    public static <T> T instantiate(Class<T> cls) {
        try {
            return cls.newInstance();
        } catch (InstantiationException e) {
            // use brute-force way of instantiation
            @SuppressWarnings("unchecked")
            final Constructor<T> constr = (Constructor<T>) cls.getConstructors()[0];
            final List<Object> params = new ArrayList<Object>();
            try {
                for (Class<?> pType : constr.getParameterTypes()) {
                    params.add(pType.isPrimitive()? Primitive.toWrapper(pType).newInstance(): null);
                }
                return constr.newInstance(params.toArray());
            } catch(IllegalAccessException e2) {
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

    public static <T> T populate(T instance, Map<String, ? extends Object> fieldValues) {
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
        } catch(IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromMap(Map<String, ?> map, Class<T> clazz) {
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
                    final Object fvalue = value instanceof Map<?, ?> && !Map.class.isAssignableFrom(fclass)?
                            fromMap((Map<String, ?>) value, fclass):
                                value == null && fclass.isPrimitive()?
                                        Primitive.getDefaultValue(fclass): value;
                    each.set(Modifier.isStatic(modifiers)? null: result, cast(fclass, fvalue));
                }
            }
            return result;
        } catch(IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private static Object cast(Class<?> clazz, Object value) {
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

    protected static <T> Map<String, Object> getFieldValues(Object object, Class<T> clazz, boolean includeTransients) {
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

    public static Map<String, Object> toMap(Object object) {
        return toMap(object, object.getClass());
    }

    public static <T> Map<String, Object> toMap(Object object, Class<T> clazz) {
        return toMap(object, clazz, false);
    }

    public static final String JDK_CLASS_REGEX = "(java|javax)\\..+";

    public static <T> Map<String, Object> toMap(Object object, Class<T> clazz, boolean includeTransients) {
        final Map<String, Object> fields = getFieldValues(object, clazz, includeTransients);
        final Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (String each: fields.keySet()) {
            Object value = fields.get(each);
            String cname = value == null? null: value.getClass().getName();
            if (value == null || cname.matches(JDK_CLASS_REGEX) || value instanceof Map || value instanceof Collection) {
                if (value instanceof Map) {
                    result.put(each, normalize((Map<?, ?>) value));
                } else {
                    result.put(each, value);
                }
            } else {System.out.println("    ---- Branching out on fieldName = " + each);
                result.put(each, toMap(value));
            }
        }
        return result;
    }

    public static <K, V> Map<Object, Object> normalize(Map<K, V> map) {
        final Map<Object, Object> result = new LinkedHashMap<Object, Object>(map.size());
        for (Entry<K, V> each: map.entrySet()) {
            final K oldKey = each.getKey();
            final Class<?> oldKeyClass = oldKey.getClass();
            final Object newKey = oldKeyClass.getName().matches(JDK_CLASS_REGEX)? oldKey: toMap(oldKey);
            final V oldValue = each.getValue();
            Object newValue = null;
            if (oldValue != null) {
                final Class<?> oldValueClass = oldValue.getClass();
                newValue = oldValueClass.getName().matches(JDK_CLASS_REGEX)? oldValue: toMap(oldValue);
            }
            result.put(newKey, newValue);
        }
        return result;
    }

}
