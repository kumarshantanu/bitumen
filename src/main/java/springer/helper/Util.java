package springer.helper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import springer.type.SqlParams;

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
        return embedReplace('$', format, values, throwOnMissing, false, null).sql;
    }

    public static SqlParams namedParamReplace(String format, Map<String, Object> values) {
        final Map<String, String> subsVals = Util.zipmap(new ArrayList<String>(values.keySet()),
                Util.repeat("?", values.size()));
        return embedReplace(':', format, subsVals, true, true, values);
    }

    public static SqlParams embedReplace(char marker, String format, Map<String, String> values, boolean throwOnMissing,
            boolean addToVals, Map<String, Object> addVals) {
        final int len = format.length();
        final StringBuilder sb = new StringBuilder(len);
        final List<Object> vals = new ArrayList<Object>();
        boolean escaped = false;
        for (int i = 0; i < len; i++) {
            final char c = format.charAt(i);
            if (c == '\\') {
                if (escaped) {
                    escaped = false;
                    sb.append(c);
                    break;
                }
                if (len - i == 1) {
                    throw new IllegalStateException("Dangling escape character found at the end of string: " + format);
                }
                escaped = true;
            } else if (c == marker) {
                if (escaped) {
                    escaped = false;
                    sb.append(c);
                    break;
                }
                if (len - i == 1) {
                    throw new IllegalStateException(
                            "Dangling marker " + marker + " found at the end of string: " + format);
                }
                final char first = format.charAt(++i);
                if (!Character.isJavaIdentifierStart(first)) {
                    throw new IllegalStateException("Illegal identifier name start '" + first + "' in: " + format);
                }
                final StringBuilder name = new StringBuilder();
                name.append(first);
                for (++i; i < len; i++) {
                    final char x = format.charAt(i);
                    if (!Character.isJavaIdentifierPart(x)) {
                        break;
                    }
                    name.append(x);
                }
                final String nameStr = name.toString();
                if (!values.containsKey(nameStr)) {
                    if (throwOnMissing) {
                        throw new IllegalArgumentException("No such key '" + nameStr + "' in: " + values.toString());
                    } else {
                        sb.append(marker).append(nameStr);
                    }
                } else {
                    sb.append(values.get(nameStr));
                    if (addToVals) {
                        vals.add(addVals.get(nameStr));
                    }
                }
                if (i < len) {
                    i--;  // push back index if not end-of-string, so that current char is picked in next pass
                }
            } else {
                escaped = false;
                sb.append(c);
            }
        }
        return new SqlParams(sb.toString(), vals.toArray());
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

    public static <T> List<T> repeat(T token, int count) {
        final List<T> result = new ArrayList<T>(count);
        for (int i = 0; i < count; i++) {
            result.add(token);
        }
        return result;
    }

    public static <K, V> List<V> getVals(Map<K, V> map, List<K> keys) {
        final List<V> vals = new ArrayList<V>(keys.size());
        for (K each: keys) {
            vals.add(map.get(each));
        }
        return vals;
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

    public static Map<String, Object> makeParamMap(Object...args) {
        return makeMap(String.class, Object.class, args);
    }

    public static <K, V> Map<K, V> zipmap(List<K> keys, List<V> vals) {
        final Map<K, V> result = new LinkedHashMap<K, V>();
        final int len = Math.min(keys.size(), vals.size());
        for (int i = 0; i < len; i++) {
            result.put(keys.get(i), vals.get(i));
        }
        return result;
    }

    public static <K, V> Map<K, V> zipmap(K[] keys, V[] vals) {
        final Map<K, V> result = new LinkedHashMap<K, V>();
        final int len = Math.min(keys.length, vals.length);
        for (int i = 0; i < len; i++) {
            result.put(keys[i], vals[i]);
        }
        return result;
    }

    public static <T> T ensureSingleItem(Collection<T> coll) {
        if (coll.isEmpty()) {
            throw new IllegalStateException("Expected collection of single item but found empty collection");
        }
        final int count = coll.size();
        if (count > 1) {
            throw new IllegalStateException("Expected collection of single item but found collection of size " + count);
        }
        return coll.iterator().next();
    }

    public static Object ensureSingleVal(Map<String, Object> map) {
        if (map.isEmpty()) {
            throw new IllegalStateException("Expected map of single pair but found empty map");
        }
        final int count = map.size();
        if (count > 1) {
            throw new IllegalStateException("Expected collection of single pair but found map of size " + count);
        }
        return map.entrySet().iterator().next().getValue();
    }

    public static <T> T firstItem(Collection<T> coll) {
        return coll.isEmpty()? null: coll.iterator().next();
    }

    public static <T> T firstItem(Collection<T> coll, T notFound) {
        return coll.isEmpty()? notFound: coll.iterator().next();
    }

    public static <T> boolean areAllNull(Collection<T> coll) {
        for (T each: coll) {
            if (each != null) {
                return false;
            }
        }
        return true;
    }

    public static void echo(String format, Object...args) {
        //System.out.printf(format, args);
    }

}
