package starfish.test;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.Properties;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;

import starfish.helper.ConnectionActivityWithoutResult;
import starfish.helper.JdbcUtil;

public class TestUtil {

    public static DataSource makeTestDataSource() {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties");
        final Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load test properties", e);
        }
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(properties.getProperty("driver.classname"));
        ds.setUrl(properties.getProperty("jdbc.url"));
        ds.setUsername(properties.getProperty("jdbc.username"));
        ds.setPassword(properties.getProperty("jdbc.password"));
        ds.setValidationQuery(properties.getProperty("validation.query"));
        JdbcUtil.withConnectionWithoutResult(ds, new ConnectionActivityWithoutResult() {
            public void execute(Connection conn) {
                JdbcUtil.update(conn, properties.getProperty("create.table.ddl"), null);
            }
        });
        return ds;
    }

}
