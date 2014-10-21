package springer.jdbc.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import springer.jdbc.IKeyHolder;

public class GeneratedKeyHolder implements IKeyHolder {

    private final List<Map<String, Object>> keyList;

    public GeneratedKeyHolder() {
        this.keyList = new ArrayList<Map<String,Object>>(1);
    }

    public GeneratedKeyHolder(List<Map<String, Object>> keyList) {
        this.keyList = keyList;
    }

    private void ensureGeneratedKeys() {
        final boolean emptyGenkey = keyList.size() == 1 && keyList.get(0).isEmpty();
        if (keyList.isEmpty() || emptyGenkey) {
            throw new IllegalStateException("No generated key found. Check whether the SQL contains identity column");
        }
    }

    @Override
    public Number get() {
        ensureGeneratedKeys();
        final int keyListSize = keyList.size();
        if (keyListSize > 1) {
            throw new IllegalStateException("Expected exactly one, but found " + keyListSize
                    + " rows of generated keys: " + keyList.toString());
        }
        return (Number) keyList.get(0).entrySet().iterator().next().getValue();
    }

    @Override
    public Number get(String columnName) {
        ensureGeneratedKeys();
        final int keyListSize = keyList.size();
        if (keyListSize > 1) {
            throw new IllegalStateException("Expected exactly one, but found " + keyListSize
                    + " rows of generated keys: " + keyList.toString());
        }
        return (Number) keyList.get(0).get(columnName);
    }

    @Override
    public Map<String, Object> getKeys() {
        ensureGeneratedKeys();
        return Collections.unmodifiableMap(keyList.get(0));
    }

    @Override
    public List<Map<String, Object>> getKeyList() {
        return Collections.unmodifiableList(keyList);
    }

}
