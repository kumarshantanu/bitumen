package springer.jdbc;

import java.sql.Connection;
import java.util.List;
import java.util.Map;

/**
 * JDBC read operations
 *
 */
public interface IJdbcRead {

    long NO_LIMIT = -1;
    boolean THROW_LIMIT_EXCEED_EXCEPTION = true, NO_LIMIT_EXCEED_EXCEPTION = false;

    /**
     * Given a JDBC connection, SQL query and parameters if any, execute query and return result rows as a list.
     * @param  conn   an active {@link java.sql.Connection} connection
     * @param  sql    SQL statement
     * @param  params SQL statement parameters
     * @return        {@link List} of row values - each row is represented by a {@link Map} of column names to values
     */
    List<Map<String, Object>> queryForList(Connection conn, String sql, Object[] params);

    /**
     * Execute SQL query and return result as a list, while honoring specified row count <i>limit</i>.
     * @param  conn   an active {@link java.sql.Connection} connection
     * @param  sql    SQL statement
     * @param  params SQL statement parameters
     * @param  limit  row count limit, after which no rows should be read
     * @param  throwLimitExceedException whether to throw exception when row count limit is exceeded
     * @return        {@link List} of row values - each row is represented by a {@link Map} of column names to values
     */
    List<Map<String, Object>> queryForList(Connection conn, String sql, Object[] params,
            long limit, boolean throwLimitExceedException);

    /**
     * Execute SQL query and return result as a list, using a row-extractor to extract each row as a list item.
     * @param  conn      an active {@link java.sql.Connection} connection
     * @param  sql       SQL statement
     * @param  params    SQL statement parameters
     * @param  extractor row extractor that extracts each row as a list item
     * @return           {@link List} of extracted rows
     */
    <T> List<T> queryForList(Connection conn, String sql, Object[] params, IRowExtractor<T> extractor);

    /**
     * Execute SQL query and return result as a list, using a row-extractor to extract each row as a list item, while
     * honoring specified row count <i>limit</i>.
     * @param  conn      an active {@link java.sql.Connection} connection
     * @param  sql       SQL statement
     * @param  params    SQL statement parameters
     * @param  extractor row extractor that extracts each row as a list item
     * @param  limit     row count limit, after which no rows should be read
     * @param  throwLimitExceedException whether to throw exception when row count limit is exceeded
     * @return           {@link List} of extracted rows
     */
    <T> List<T> queryForList(Connection conn, String sql, Object[] params, IRowExtractor<T> extractor,
            long limit, boolean throwLimitExceedException);

    /**
     * Execute SQL query and return result as a map, using a key extractor and a value extractor to extract each row as
     * a pair of key and value.
     * @param  conn           an active {@link java.sql.Connection} connection
     * @param  sql            SQL statement
     * @param  params         SQL statement parameters
     * @param  keyExtractor   key extractor that extracts the key from a row - extracted key must be unique
     * @param  valueExtractor value extractor that extracts the value from a row
     * @return                {@link Map} of extracted key-value pairs
     */
    <K, V> Map<K, V> queryForMap(Connection conn, String sql, Object[] params, IRowExtractor<K> keyExtractor,
            IRowExtractor<V> valueExtractor);

    /**
     * Execute SQL query and return result as a map, using a key extractor and a value extractor to extract each row as
     * a pair of key and value, while honoring specified row count <i>limit</i>.
     * @param  conn           an active {@link java.sql.Connection} connection
     * @param  sql            SQL statement
     * @param  params         SQL statement parameters
     * @param  keyExtractor   key extractor that extracts the key from a row - extracted key must be unique
     * @param  valueExtractor value extractor that extracts the value from a row
     * @param  limit          row count limit, after which no rows should be read
     * @param  throwLimitExceedException whether to throw exception when row count limit is exceeded
     * @return                {@link Map} of extracted key-value pairs
     */
    <K, V> Map<K, V> queryForMap(Connection conn, String sql, Object[] params, IRowExtractor<K> keyExtractor,
            IRowExtractor<V> valueExtractor, long limit, boolean throwLimitExceedException);

    /**
     * Execute SQL query and return result as determined by specified result-set extractor.
     * @param  conn      an active {@link java.sql.Connection} connection
     * @param  sql       SQL statement
     * @param  params    SQL statement parameters
     * @param  extractor result-set extractor that determines the return value based on the query result-set
     * @return           value determined by result-set extractor
     */
    <T> T queryCustom(Connection conn, String sql, Object[] params, IResultSetExtractor<T> extractor);

}
