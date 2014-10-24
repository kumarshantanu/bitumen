package springer.jdbc;

import java.util.List;
import java.util.Map;

/**
 * Holder for keys generated during execution of SQL (typically INSERT) statement.
 *
 */
public interface IKeyHolder {

    /**
     * Get the only generated key. Throw {@link IllegalStateException} if generated key count is not exactly one.
     * @return generated key
     */
    Number get();

    /**
     * Get the generated key identified by specified column name. Typically required when statement generated multiple
     * keys.
     * @param  columnName column name that the generated key is identified by
     * @return            generated key
     */
    Number get(String columnName);

    /**
     * Get all generated keys as a map, where the key is the column name and value is the generated key.
     * @return map of column names to corresponding generated keys
     */
    Map<String, Object> getKeys();

    /**
     * Get all generated keys as a list of maps, where the keys are the column names and values are generated keys.
     * This call may be useful when a SQL statement affects multiple rows.
     * @return list of maps of column names to corresponding generated keys
     */
    List<Map<String, Object>> getKeyList();

}
