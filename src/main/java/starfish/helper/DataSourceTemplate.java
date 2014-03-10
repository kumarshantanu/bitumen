package starfish.helper;

import javax.sql.DataSource;

public class DataSourceTemplate {

    public final DataSource dataSource;

    public DataSourceTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <V> V withConnection(ConnectionActivity<V> activity) {
        return JdbcUtil.withConnection(dataSource, activity);
    }

    public void withConnectionNoResult(ConnectionActivityNoResult activity) {
        JdbcUtil.withConnectionNoResult(dataSource, activity);
    }

    public <V> V withTransaction(ConnectionActivity<V> activity, int txnIsolation) {
        return JdbcUtil.withTransaction(dataSource, txnIsolation, activity);
    }

    public <V> V withTransaction(ConnectionActivity<V> activity) {
        return JdbcUtil.withTransaction(dataSource, activity);
    }

    public void withTransactionNoResult(ConnectionActivityNoResult activity, int txnIsolation) {
        JdbcUtil.withTransactionNoResult(dataSource, txnIsolation, activity);
    }

    public void withTransactionNoResult(ConnectionActivityNoResult activity) {
        JdbcUtil.withTransactionNoResult(dataSource, activity);
    }

}
