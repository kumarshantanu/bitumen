package net.sf.bitumen.jdbc.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import net.sf.bitumen.jdbc.IKeyHolder;

/**
 * Default implementation of {@link IKeyHolder}.
 *
 */
public class GeneratedKeyHolder implements IKeyHolder {

    /**
     * The list of generated keys to hold.
     */
    private final List<Map<String, Object>> keyList;

    /**
     * Construct instance that has no generated keys.
     */
    public GeneratedKeyHolder() {
        this.keyList = new ArrayList<Map<String, Object>>(1);
    }

    /**
     * Construct instance with valid generated keys.
     * @param  generatedKeyList list of generated keys
     */
    public GeneratedKeyHolder(final List<Map<String, Object>> generatedKeyList) {
        if (generatedKeyList == null || generatedKeyList.isEmpty()) {
            throw new IllegalArgumentException(
                    "Expected non-empty list of generated keys, found: " + String.valueOf(generatedKeyList));
        }
        this.keyList = generatedKeyList;
    }

    /**
     * Ensure that one or more generated keys actually exist. Throw {@code IllegalStateException} otherwise.
     */
    private void ensureGeneratedKeys() {
        final boolean emptyGenkey = keyList.size() == 1 && keyList.get(0).isEmpty();
        if (keyList.isEmpty() || emptyGenkey) {
            throw new IllegalStateException("No generated key found. Check whether the SQL contains identity column");
        }
    }

    @Override
    public final Number get() {
        ensureGeneratedKeys();
        final int keyListSize = keyList.size();
        if (keyListSize > 1) {
            throw new IllegalStateException("Expected exactly one, but found " + keyListSize
                    + " rows of generated keys: " + keyList.toString());
        }
        return (Number) keyList.get(0).entrySet().iterator().next().getValue();
    }

    @Override
    public final Number get(final String columnName) {
        ensureGeneratedKeys();
        final int keyListSize = keyList.size();
        if (keyListSize > 1) {
            throw new IllegalStateException("Expected exactly one, but found " + keyListSize
                    + " rows of generated keys: " + keyList.toString());
        }
        return (Number) keyList.get(0).get(columnName);
    }

    @Override
    public final Map<String, Object> getKeys() {
        ensureGeneratedKeys();
        return Collections.unmodifiableMap(keyList.get(0));
    }

    @Override
    public final List<Map<String, Object>> getKeyList() {
        return Collections.unmodifiableList(keyList);
    }

}
