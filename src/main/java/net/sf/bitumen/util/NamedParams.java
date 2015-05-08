package net.sf.bitumen.util;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class NamedParams<K> {

    private final String text;
    private final K[] keys;

    public NamedParams(final String text, final K[] keys) {
        this.text = text;
        this.keys = keys;
    }

    public String getText() {
        return text;
    }

    public Object[] getKeys() {
        return keys;
    }

    /**
     * Given a params map, lay out the values in the same order as keys and return it.
     * @param paramMap  the params map
     * @return          list of parameter values in the same order as keys
     */
    public List<?> getParams(final Map<K, ?> paramMap) {
        final ArrayList<Object> result = new ArrayList<Object>(keys.length);
        for (int i = 0; i < keys.length; i++) {
            final K k = keys[i];
            if (!paramMap.containsKey(k)) {
                throw new IllegalArgumentException("No value found for key: " + String.valueOf(k));
            }
            result.add(paramMap.get(k));
        }
        return result;
    }

    public static interface Replacement {

        public boolean containsKey(Object key);

        public String get(String key);

    }

    public static Replacement replaceWith(final Map<String, String> map) {
        return new Replacement() {
            @Override
            public String get(String key) {
                return map.get(key);
            }

            @Override
            public boolean containsKey(Object key) {
                return map.containsKey(key);
            }

            @Override
            public String toString() {
                return map.toString();
            }
        };
    }

    public static Replacement replaceWith(final String token) {
        return new Replacement() {
            @Override
            public String get(String key) {
                return token;
            }

            @Override
            public boolean containsKey(Object key) {
                return true;
            }

            @Override
            public String toString() {
                return token;
            }
        };
    }

    public static final IFunction1<String, String> IDENTITY_KEY_ENCODER = new IFunction1<String, String>() {
        @Override
        public String invoke(final String arg) {
            return arg;
        }
    };

    /**
     * Parse a 'format' string, identify variables and replace them with specified values, and do few other useful
     * things. '\' is the escape character; '\\' represents a single '\' in the 'format' string. Variable names are
     * prefixed with a 'marker' character, and follow Java variable naming rules.
     * @param  marker         character that is prefixed to every variable name in the 'format' string
     * @param  format         the 'format' string
     * @param  replaceWith    values to replace variables with
     * @param  throwOnMissing whether throw exception on finding missing variable names (useful for partial rendering)
     * @param  addToKeys      whether variables to be added to <tt>keys</tt>
     * @param  keyEncoder     converts variable name to key
     * @return                {@link NamedParams} instance
     */
    public static <K> NamedParams<K> replace(final char marker, final String format, final Replacement replaceWith,
            final boolean throwOnMissing, final boolean addToKeys, final IFunction1<String, K> keyEncoder) {
        final int len = format.length();
        final StringBuilder sb = new StringBuilder(len);
        final List<K> keys = addToKeys? new ArrayList<K>(): null;
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
                i++; // hop to next char
                while (i < len) {
                    final char x = format.charAt(i);
                    if (!(Character.isJavaIdentifierPart(x) || x == '-' /* allow dash for Clojure keywords */)) {
                        break;
                    }
                    name.append(x);
                    i++; // hop to next char
                }
                final String nameStr = name.toString();
                if (!replaceWith.containsKey(nameStr)) {
                    if (throwOnMissing) {
                        throw new IllegalArgumentException("No such key '" + nameStr + "' in: " + replaceWith);
                    } else {
                        sb.append(marker).append(nameStr);
                    }
                } else {
                    sb.append(replaceWith.get(nameStr));
                    if (addToKeys) {
                        keys.add(keyEncoder.invoke(nameStr));
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
        @SuppressWarnings("unchecked")
        final K[] ks = (K[]) new Object[0];
        return new NamedParams<K>(sb.toString(), addToKeys? keys.toArray(ks): null);
    }

    /**
     * Given a 'format' string containing variables prefixed with '$' character, replace the variables with
     * corresponding values in specified map.
     * @param  format         the 'format string
     * @param  values         map of variable names to values
     * @param  throwOnMissing whether throw exception when encountered missing variable in value map
     * @return                rendered string after replacing variables with their corresponding values
     */
    public static String groovyReplace(final String format, final Map<String, String> values,
            final boolean throwOnMissing) {
        return replace('$', format, replaceWith(values), throwOnMissing, false, null).getText();
    }

    /**
     * Replace named parameters (i.e. variables with marker character ':') with placeholder '?' storing the key names.
     * @param  format the SQL statement with embedded named parameters, e.g. "SELECT * FROM emp WHERE id = :id"
     * @return        {@link NamedParams<String>} instance from derived SQL statement and parameter values
     */
    public static NamedParams<String> jdbcReplace(final String format) {
        return replace(':', format, replaceWith("?"), true, true, IDENTITY_KEY_ENCODER);
    }

    /**
     * Replace named parameters (i.e. variables with marker character ':') with placeholder '?' storing the key names.
     * Use specified key-encoder to encode the variable names as keys.
     * @param  format     the SQL statement with embedded named parameters, e.g. "SELECT * FROM emp WHERE id = :id"
     * @param  keyEncoder the key encoder
     * @return        {@link NamedParams<String>} instance from derived SQL statement and parameter values
     */
    public static <K> NamedParams<K> jdbcReplace(final String format, final IFunction1<String, K> keyEncoder) {
        return replace(':', format, replaceWith("?"), true, true, keyEncoder);
    }

}
