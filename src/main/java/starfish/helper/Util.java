package starfish.helper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class Util {

    public static <T> T singleResult(List<T> result) {
        if (result == null || result.isEmpty()) {
            throw new IllegalStateException("Expected exactly one result item but found empty");
        }
        if (result.size() > 1) {
            throw new IllegalStateException("Expected exactly one result item but more than one");
        }
        return result.get(0);
    }

    public static void assertNotNull(Object obj, String msg) {
        if (obj == null) {
            throw new IllegalArgumentException(msg);
        }
    }

    public static <T> T notNull(T obj, String msg) {
        assertNotNull(obj, msg);
        return obj;
    }

    public static Timestamp now() {
        return new Timestamp(new Date().getTime());
    }

    public static long newVersion() {
        return UUID.randomUUID().getMostSignificantBits();
    }

    public static Long[] newVersions(int n) {
        Long[] result = new Long[n];
        for (int i = 0; i < n; i++) {
            result[i] = newVersion();
        }
        return result;
    }

    public static String groovyReplace(String format, Map<String, String> values, boolean throwOnMissing) {
        final int len = format.length();
        final StringBuilder sb = new StringBuilder(len);
        boolean escaped = false;
        for (int i = 0; i < len; i++) {
            final char c = format.charAt(i);
            switch (c) {
            case '\\':
                if (escaped) {
                    escaped = false;
                    sb.append(c);
                    break;
                }
                if (len - i == 1) {
                    throw new IllegalStateException("Dangling escape character found at the end of string: " + format);
                }
                escaped = true;
                break;

            case '$':
                if (escaped) {
                    escaped = false;
                    sb.append(c);
                    break;
                }
                if (len - i == 1) {
                    throw new IllegalStateException("Dangling variable marker $ found at the end of string: " + format);
                }
                final char first = format.charAt(++i);
                if (!Character.isJavaIdentifierStart(first)) {
                    throw new IllegalStateException("Illegal identifier name start '" + first + "' in: " + format);
                }
                final StringBuilder name = new StringBuilder();
                name.append(first);
                for (char x = format.charAt(++i); i < len && Character.isJavaIdentifierPart(x); i++, x = format.charAt(i)) {
                    name.append(x);
                }
                final String nameStr = name.toString();
                if (!values.containsKey(nameStr)) {
                    if (throwOnMissing) {
                        throw new IllegalArgumentException("No such key '" + nameStr + "' in: " + values.toString());
                    } else {
                        sb.append('$').append(nameStr);
                    }
                } else {
                    sb.append(values.get(nameStr));
                    i -= (i == len? 0: 1);  // push back index if not end-of-string, so current char is picked in next pass
                }
                break;

            default:
                escaped = false;
                sb.append(c);
                break;
            }
        }
        return sb.toString();
    }

    public static <T> boolean equals(T one, T two) {
        if (one == two) return true;
        return (one == null? two.equals(one): one.equals(two));
    }

    public static String repeat(CharSequence token, int count, CharSequence delimiter) {
        StringBuilder sb = new StringBuilder((token.length() + delimiter.length()) * count);
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(token);
        }
        return sb.toString();
    }

    public static <T> List<T> removeNull(List<T> withNull) {
        final List<T> result = new ArrayList<T>();
        for (T each: withNull) {
            if (each != null) {
                result.add(each);
            }
        }
        return result;
    }

    public static <T, U> List<U> removeNull(List<T> withNull, List<U> another) {
        final List<U> result = new ArrayList<U>();
        final int len = Math.min(withNull.size(), another.size());
        for (int i = 0; i < len; i++) {
            if (withNull.get(i) != null) {
                result.add(another.get(i));
            }
        }
        return result;
    }

    public static <T, U> List<U> mergeNull(List<T> withNull, List<U> newList) {
        final List<U> result = new ArrayList<U>(withNull.size());
        int i = 0;
        for (T each: withNull) {
            result.add(each == null? null: newList.get(i++));
        }
        return result;
    }

    public static <K, V> Object[] argsArray(Map<K, V> map) {
        final Object[] result = new Object[map.size() * 2];
        int i = 0;
        for (K key: map.keySet()) {
            result[i++] = key;
            result[i++] = map.get(key);
        }
        return result;
    }

    public static <K, V> Map<K, V> makeMap(Class<K> kt, Class<V> vt, Object...args) {
        final int len = args.length;
        if (len % 2 == 1) {
            throw new IllegalArgumentException("Expected even number of args, but found " + len);
        }
        final Map<K, V> result = new LinkedHashMap<K, V>(len / 2);
        for (int i = 0; i < len; i += 2) {
            result.put(kt.cast(args[i]), vt.cast(args[i + 1]));
        }
        return result;
    }

    public static <K, V> Map<K, V> zipmap(List<K> keys, List<V> vals) {
        final Map<K, V> result = new LinkedHashMap<K, V>();
        final int len = Math.min(keys.size(), vals.size());
        for (int i = 0; i < len; i++) {
            result.put(keys.get(i), vals.get(i));
        }
        return result;
    }

    public static <T> boolean areAllNull(Collection<T> coll) {
        for (T each: coll) {
            if (each != null) {
                return false;
            }
        }
        return true;
    }

}
