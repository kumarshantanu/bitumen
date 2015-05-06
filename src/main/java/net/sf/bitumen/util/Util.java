package net.sf.bitumen.util;

import java.io.PrintStream;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.UUID;

/**
 * General utility methods.
 *
 */
public final class Util {

    /** Private constructor for utility class. */
    private Util() {
        // do nothing
    }

    /**
     * Assert that specified object is not <tt>null</tt> and return it. If <tt>null</tt>, throw
     * {@link IllegalArgumentException} with specified message.
     * @param obj object to be asserted not <tt>null</tt>
     * @param msg error message to use in the exception thrown
     */
    public static void assertNotNull(final Object obj, final String msg) {
        if (obj == null) {
            throw new IllegalArgumentException(msg);
        }
    }

    /**
     * Assert that specified object is not <tt>null</tt> and return the same. If <tt>null</tt> then throw
     * {@link IllegalArgumentException} with a specified error message.
     * @param  <T> type of the object
     * @param  obj object to be tested
     * @param  msg error message
     * @return     same object passed for testing
     */
    public static <T> T notNull(final T obj, final String msg) {
        assertNotNull(obj, msg);
        return obj;
    }

    /**
     * Return current timestamp in milliseconds.
     * @return current timestamp in milliseconds
     */
    public static Timestamp now() {
        return new Timestamp(new Date().getTime());
    }

    /**
     * Obtain a random new version for a key-value tuple.
     * @return new version
     */
    public static long newVersion() {
        return UUID.randomUUID().getMostSignificantBits();
    }

    /**
     * Obtain specified number of new versions, typically for multiple key-value tuples.
     * @param  n count of new versions required
     * @return   array of new versions
     */
    public static Long[] newVersions(final int n) {
        Long[] result = new Long[n];
        for (int i = 0; i < n; i++) {
            result[i] = newVersion();
        }
        return result;
    }

    /**
     * Generic implement of <tt>.equals(Object)</tt> for two arbitrary objects.
     * @param  <T> type of objects
     * @param  one first object
     * @param  two second object
     * @return     <tt>true</tt> if both are equal, <tt>false</tt> otherwise
     */
    public static <T> boolean equals(final T one, final T two) {
        if (one == null || two == null) {
            return false;
        }
        if (one == two) {
            return true;
        }
        return one.equals(two);
    }

    /**
     * Repeat a given string token specified number of times using a specified delimiter, and return the concatenated
     * string.
     * @param  token     string token to repeat
     * @param  count     number of times to repeat
     * @param  delimiter string to delimit with
     * @return           final concatenated string
     */
    public static String repeat(final CharSequence token, final int count, final CharSequence delimiter) {
        StringBuilder sb = new StringBuilder((token.length() + delimiter.length()) * count);
        for (int i = 0; i < count; i++) {
            if (i > 0) {
                sb.append(delimiter);
            }
            sb.append(token);
        }
        return sb.toString();
    }

    /**
     * Repeat given object specified number of times and return a list of repetitions.
     * @param  <T>   object type
     * @param  token object to repeat
     * @param  count number of times to repeat
     * @return       list of repetitions
     */
    public static <T> List<T> repeat(final T token, final int count) {
        final List<T> result = new ArrayList<T>(count);
        for (int i = 0; i < count; i++) {
            result.add(token);
        }
        return result;
    }

    /**
     * For a given map and list of keys, find corresponding values and return as a list.
     * @param  <K>  key type
     * @param  <V>  value type
     * @param  map  map of keys and values
     * @param  keys list of keys
     * @return      list of values
     */
    public static <K, V> List<V> getVals(final Map<K, V> map, final List<K> keys) {
        final List<V> vals = new ArrayList<V>(keys.size());
        for (K each: keys) {
            vals.add(map.get(each));
        }
        return vals;
    }

    /**
     * Layout out elements (both key and value) in a given map in flat linear fashion and return as an <tt>Object</tt>
     * array. For example, if you pass {1 => 2, 3 => 4} you get back {1, 2, 3, 4}.
     * @param  map the key value map
     * @return     flattened keys and values
     */
    public static Object[] argsArray(final Map<?, ?> map) {
        final Object[] result = new Object[map.size() * 2];
        int i = 0;
        for (Entry<?, ?> entry: map.entrySet()) {
            result[i++] = entry.getKey();
            result[i++] = entry.getValue();
        }
        return result;
    }

    /**
     * Create SQL params map using supplied String keys and corresponding values.
     * @param  args keys/values in a flat sequence
     * @return      map of String keys to corresponding values
     */
    public static Map<String, Object> makeParamMap(final Object...args) {
        return MapBuilder.putAll(new LinkedHashMap<String, Object>(), args);
    }

    /**
     * Given lists of keys and values, create a map out of them and return it.
     * @param  <K>  key type
     * @param  <V>  value type
     * @param  keys keys
     * @param  vals values
     * @return      map of keys and values
     */
    public static <K, V> Map<K, V> zipmap(final List<K> keys, final List<V> vals) {
        final Map<K, V> result = new LinkedHashMap<K, V>();
        final int len = Math.min(keys.size(), vals.size());
        for (int i = 0; i < len; i++) {
            result.put(keys.get(i), vals.get(i));
        }
        return result;
    }

    /**
     * Given arrays of keys and values, create a map out of them and return it.
     * @param  <K>  key type
     * @param  <V>  value type
     * @param  keys keys
     * @param  vals values
     * @return      map of keys and values
     */
    public static <K, V> Map<K, V> zipmap(final K[] keys, final V[] vals) {
        final Map<K, V> result = new LinkedHashMap<K, V>();
        final int len = Math.min(keys.length, vals.length);
        for (int i = 0; i < len; i++) {
            result.put(keys[i], vals[i]);
        }
        return result;
    }

    /**
     * Given a collection, make sure it has only one item and return the item.
     * @param  <T>  element type in the collection
     * @param  coll the collection
     * @return      item from the collection
     */
    public static <T> T ensureSingleItem(final Collection<T> coll) {
        if (coll.isEmpty()) {
            throw new IllegalStateException("Expected collection of single item but found empty collection");
        }
        final int count = coll.size();
        if (count > 1) {
            throw new IllegalStateException("Expected collection of single item but found collection of size " + count);
        }
        return coll.iterator().next();
    }

    /**
     * Given a map, make sure it has only one key-value pair and return the value.
     * @param  map the map
     * @return     value from the map
     */
    public static Object ensureSingleVal(final Map<String, Object> map) {
        if (map.isEmpty()) {
            throw new IllegalStateException("Expected map of single pair but found empty map");
        }
        final int count = map.size();
        if (count > 1) {
            throw new IllegalStateException("Expected collection of single pair but found map of size " + count);
        }
        return map.entrySet().iterator().next().getValue();
    }

    /**
     * Return first item if given collection has one or more elements, <tt>null</tt> if collection is empty.
     * @param  <T>  element type in the collection
     * @param  coll the collection
     * @return      first element of the collection.
     */
    public static <T> T firstItem(final Collection<T> coll) {
        return coll.isEmpty() ? null : coll.iterator().next();
    }

    /**
     * Return first item if given collection has one or more elements, specified default value if collection is empty.
     * @param  <T>      element type in the collection
     * @param  coll     the collection
     * @param  notFound default value
     * @return          first element of collection, or the default value
     */
    public static <T> T firstItem(final Collection<T> coll, final T notFound) {
        return coll.isEmpty() ? notFound : coll.iterator().next();
    }

    /**
     * Return <tt>true</tt> if all elements in given collection are <tt>null</tt>, <tt>false</tt> otherwise.
     * @param  coll the collection
     * @return      <tt>true</tt> or <tt>false</tt>
     */
    public static boolean areAllNull(final Collection<?> coll) {
        for (Object each: coll) {
            if (each != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Echo given format and arguments to STDOUT. Useful for debugging.
     * @param format string format
     * @param args   format arguments
     */
    public static void echo(final String format, final Object...args) {
        //System.out.printf(format, args);
    }

    /**
     * Swallow exception and print to a stream.
     * @param e  exception or error
     * @param ps output stream
     */
    public static void swallow(final Throwable e, final PrintStream ps) {
        ps.println("Swallowing exception: " + e);
        e.printStackTrace(ps);
    }

    /**
     * Swallow exception and print to STDERR.
     * @param e exception or error
     */
    public static void swallow(final Throwable e) {
        swallow(e, System.err);
    }

    /**
     * Return class name of a given object.
     * @param  instance any object or <tt>null</tt>
     * @return          name of the object's class
     */
    public static String getClassName(final Object instance) {
        return (instance == null) ? "NULL" : instance.getClass().toString();
    }

}
