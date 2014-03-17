package springer.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import springer.KeyHolder;

public class GeneratedKeyHolder implements KeyHolder {

    private final List<Map<String, Object>> keyList;

    public GeneratedKeyHolder() {
        this.keyList = new ArrayList<Map<String,Object>>(1);
    }

    public GeneratedKeyHolder(List<Map<String, Object>> keyList) {
        this.keyList = keyList;
    }

    private void ensureGeneratedKeys() {
        if (keyList.isEmpty() || (keyList.size() == 1 && keyList.get(0).isEmpty())) {
            throw new IllegalStateException("No generated key found. Check whether the SQL contains identity column");
        }
    }

    @Override
    public Number get() {
        ensureGeneratedKeys();
        final int keyListSize = keyList.size();
        final int keyMapSize = (keyListSize == 1? keyList.get(0).size(): 0);
        if (keyListSize > 1 || keyMapSize > 1) {
            throw new IllegalStateException("Expected exactly one, but found " + Math.max(keyListSize, keyMapSize)
                    + " generated keys");
        }
        return (Number) keyList.get(0).entrySet().iterator().next().getValue();
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
