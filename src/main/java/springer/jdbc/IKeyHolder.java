package springer.jdbc;

import java.util.List;
import java.util.Map;

public interface IKeyHolder {

    Number get();

    Number get(String ColumnName);

    Map<String, Object> getKeys();

    List<Map<String, Object>> getKeyList();

}
