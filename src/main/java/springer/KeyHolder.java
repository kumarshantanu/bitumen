package springer;

import java.util.List;
import java.util.Map;

public interface KeyHolder {

    public Number get();

    public Number get(String ColumnName);

    public Map<String, Object> getKeys();

    public List<Map<String, Object>> getKeyList();

}
