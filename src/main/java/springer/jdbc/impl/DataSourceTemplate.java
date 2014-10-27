package springer.jdbc.impl;

import javax.sql.DataSource;

/**
 * {@link DataSource} operations.
 *
 */
public class DataSourceTemplate {

    /**
     * Encapsulated {@link DataSource} to act upon.
     */
    private final DataSource dataSource;

    /**
     * Construct from {@link DataSource} instance.
     * @param  jdbcDataSource {@link DataSource} instance
     */
    public DataSourceTemplate(final DataSource jdbcDataSource) {
        this.dataSource = jdbcDataSource;
    }

    /**
     * Execute an activity using a {@link java.sql.Connection} instance and clean it up afterward.
     * @param  <V>      return type of the activity
     * @param  activity the activity
     * @return          whatever that activity returns
     * @see             JdbcUtil#withConnection(DataSource, IConnectionActivity)
     */
    public final <V> V withConnection(final IConnectionActivity<V> activity) {
        return JdbcUtil.withConnection(dataSource, activity);
    }

    /**
     * Execute an activity using a {@link java.sql.Connection} instance and clean it up afterward.
     * @param  activity the activity
     * @see             JdbcUtil#withConnectionNoResult(DataSource, IConnectionActivityNoResult)
     */
    public final void withConnectionNoResult(final IConnectionActivityNoResult activity) {
        JdbcUtil.withConnectionNoResult(dataSource, activity);
    }

    /**
     * Execute a transaction using a {@link java.sql.Connection} instance and specified transaction isolation level,
     * and clean it up afterward.
     * @param  <V>          return type of transaction activity
     * @param  activity     transaction activity
     * @param  txnIsolation transaction isolation level as specified in {@link java.sql.Connection}
     * @return              whatever transaction activity returns
     * @see                 JdbcUtil#withTransaction(DataSource, int, IConnectionActivity)
     * @see                 java.sql.Connection
     */
    public final <V> V withTransaction(final IConnectionActivity<V> activity, final int txnIsolation) {
        return JdbcUtil.withTransaction(dataSource, txnIsolation, activity);
    }

    /**
     * Execute a transaction using a {@link java.sql.Connection} instance and default transaction isolation level,
     * and clean it up afterward.
     * @param  <V>      return type of transaction activity
     * @param  activity transaction activity
     * @return          whatever transaction activity returns
     * @see             JdbcUtil#withTransaction(DataSource, IConnectionActivity)
     */
    public final <V> V withTransaction(final IConnectionActivity<V> activity) {
        return JdbcUtil.withTransaction(dataSource, activity);
    }

    /**
     * Execute a transaction using a {@link java.sql.Connection} instance and specified transaction isolation level,
     * and clean it up afterward.
     * @param  activity     transaction activity
     * @param  txnIsolation transaction isolation level as specified in {@link java.sql.Connection}
     * @see                 JdbcUtil#withTransactionNoResult(DataSource, int, IConnectionActivityNoResult)
     * @see                 java.sql.Connection
     */
    public final void withTransactionNoResult(final IConnectionActivityNoResult activity, final int txnIsolation) {
        JdbcUtil.withTransactionNoResult(dataSource, txnIsolation, activity);
    }

    /**
     * Execute a transaction using a {@link java.sql.Connection} instance and default transaction isolation level,
     * and clean it up afterward.
     * @param  activity transaction activity
     * @see             JdbcUtil#withTransactionNoResult(DataSource, IConnectionActivityNoResult)
     */
    public final void withTransactionNoResult(final IConnectionActivityNoResult activity) {
        JdbcUtil.withTransactionNoResult(dataSource, activity);
    }

}
