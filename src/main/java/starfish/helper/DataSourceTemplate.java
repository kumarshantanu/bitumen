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
        JdbcUtil.withConnectionWithoutResult(dataSource, activity);
    }

}
