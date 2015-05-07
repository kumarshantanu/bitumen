package net.sf.bitumen.jdbc;

import java.sql.Connection;
import java.util.Collection;

/**
 * JDBC write operations.
 *
 */
public interface IJdbcWrite {

    // generate key

    /**
     * Execute a SQL statement that generates one or more keys, and returns a key holder to access those keys.
     * @param  conn   a JDBC {@link Connection} to execute statement on
     * @param  sql    SQL statement
     * @param  params parameters for the SQL statement
     * @return        key holder to access generated keys
     */
    IKeyHolder genkey(Connection conn, String sql, Iterable<?> params);

    // update

    /**
     * Execute an update (e.g. INSERT, UPDATE, DELETE, CREATE TABLE etc.) SQL statement and return the number of rows
     * affected.
     * @param  conn   a JDBC {@link Connection} to execute statement on
     * @param  sql    SQL statement
     * @param  params parameters for the SQL statement
     * @return        number of rows affected
     */
    int update(Connection conn, String sql, Iterable<?> params);

    /**
     * Execute a batch update (e.g. INSERT, UPDATE, DELETE etc.) SQL statement and return the number of rows affected
     * for each parameter set in the batch. The same SQL statement is shared among all parameter sets in the batch.
     * @param  conn        a JDBC {@link Connection} to execute statement on
     * @param  sql         SQL statement
     * @param  paramsBatch parameter sets for the SQL statement
     * @return             number of rows affected for each parameter set in the batch
     */
    int[] batchUpdate(Connection conn, String sql, Collection<? extends Iterable<?>> paramsBatch);

}
