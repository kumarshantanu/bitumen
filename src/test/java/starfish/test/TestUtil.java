package starfish.test;

import java.sql.Connection;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import starfish.helper.ConnectionActivityWithoutResult;
import starfish.helper.JdbcUtil;

public class TestUtil {

    public static String H2_DDL = "CREATE TABLE session ("
            + "id INT PRIMARY KEY AUTO_INCREMENT,"
            + "key VARCHAR(30),"
            + "value TEXT,"
            + "version BIGINT,"
            + "updated DATETIME"
            + ")";

    public static DataSource makeH2DataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName("org.h2.Driver");
        ds.setUrl("jdbc:h2:mem:test");
        ds.setUsername("sa");
        ds.setValidationQuery("SELECT 1");
        JdbcUtil.withConnectionNoResult(ds, new ConnectionActivityWithoutResult() {
            public void execute(Connection conn) {
                JdbcUtil.update(conn, H2_DDL, null);
            }
        });
        return ds;
    }

}
