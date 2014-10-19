package springer.jdbc.helper;

import javax.sql.DataSource;

public class DataSourceTemplate {

    public final DataSource dataSource;

    public DataSourceTemplate(final DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public <V> V withConnection(IConnectionActivity<V> activity) {
        return JdbcUtil.withConnection(dataSource, activity);
    }

    public void withConnectionNoResult(IConnectionActivityNoResult activity) {
        JdbcUtil.withConnectionNoResult(dataSource, activity);
    }

    public <V> V withTransaction(IConnectionActivity<V> activity, int txnIsolation) {
        return JdbcUtil.withTransaction(dataSource, txnIsolation, activity);
    }

    public <V> V withTransaction(IConnectionActivity<V> activity) {
        return JdbcUtil.withTransaction(dataSource, activity);
    }

    public void withTransactionNoResult(IConnectionActivityNoResult activity, int txnIsolation) {
        JdbcUtil.withTransactionNoResult(dataSource, txnIsolation, activity);
    }

    public void withTransactionNoResult(IConnectionActivityNoResult activity) {
        JdbcUtil.withTransactionNoResult(dataSource, activity);
    }

}
