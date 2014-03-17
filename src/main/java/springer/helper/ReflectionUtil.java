package springer.helper;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ReflectionUtil {

    public static class Primitive {

        // these get initialized to their default values
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
                throw new IllegalArgumentException(
                    "Class type " + clazz + " not supported");
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
                throw new IllegalArgumentException(
                    "Class type " + clazz + " not supported");
            }
        }

    }

    private static <T> T instantiate(Class<T> cls, Map<String, ? extends Object> args) {
        // Create instance of the given class
        @SuppressWarnings("unchecked")
        final Constructor<T> constr = (Constructor<T>) cls.getConstructors()[0];
        final List<Object> params = new ArrayList<Object>();
        try {
            for (Class<?> pType : constr.getParameterTypes()) {
                params.add((pType.isPrimitive()) ? Primitive.toWrapper(pType).newInstance() : null);
            }
            return constr.newInstance(params.toArray());
        } catch(IllegalAccessException e) {
            throw new IllegalArgumentException(e);
        } catch (InvocationTargetException e) {
            throw new IllegalArgumentException(e);
        } catch (InstantiationException e) {
            throw new IllegalArgumentException(e);
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromMap(Map<String, ?> map, Class<T> clazz) {
        final Field[] fields = clazz.getDeclaredFields();
        try {
            final T result = instantiate(clazz, map);
            for (Field each: fields) {
                final int modifiers = each.getModifiers();
                each.setAccessible(true);
                final String fname = each.getName();
                final Object value = map.get(fname);
                final Class<?> fclass = each.getType();
                if (!Modifier.isFinal(modifiers)) {
                    final Object fvalue = value instanceof Map<?, ?> && !fclass.equals(Map.class)?
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

    protected static <T> Map<String, Object> getFields(T object) {
        return getFieldValues(object, object.getClass());
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

    protected static <T> Map<String, Object> getFieldValues(Object object, Class<T> clazz) {
        if (!clazz.isInstance(object)) {
            throw new IllegalArgumentException("Object " + object + " is not an instance of class " + clazz.getName());
        }
        final Field[] fields = clazz.getDeclaredFields();
        final Map<String, Object> data = new LinkedHashMap<String, Object>(fields.length);
        for (Field each: fields) {
            each.setAccessible(true);
            Object val;
            try {
                val = each.get(object);
                data.put(each.getName(), val);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException(e);
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
        final Map<String, Object> fields = getFieldValues(object, clazz);
        final Map<String, Object> result = new LinkedHashMap<String, Object>();
        for (String each: fields.keySet()) {
            Object value = fields.get(each);
            String cname = value == null? null: value.getClass().getName();
            result.put(each, value == null || cname.matches("(java|javax)\\..+")? value: toMap(value));
        }
        return result;
    }

}
