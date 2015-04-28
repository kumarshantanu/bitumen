package net.sf.bitumen.test.helper;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import javax.sql.DataSource;

import net.sf.bitumen.jdbc.impl.DataSourceTemplate;
import net.sf.bitumen.jdbc.impl.DefaultJdbcRead;
import net.sf.bitumen.jdbc.impl.DefaultJdbcWrite;
import net.sf.bitumen.jdbc.impl.IConnectionActivity;
import net.sf.bitumen.jdbc.impl.IConnectionActivityNoResult;
import net.sf.bitumen.jdbc.impl.JdbcUtil;
import net.sf.bitumen.jdbc.kv.impl.TableMetadata;

import org.apache.commons.dbcp.BasicDataSource;

public class TestUtil {

    public static final TableMetadata meta = TableMetadata.create("session", "skey", "value", "version",
            "created", "updated");

    public static Properties loadProperties() {
        InputStream in = Thread.currentThread().getContextClassLoader().getResourceAsStream("database.properties");
        final Properties properties = new Properties();
        try {
            properties.load(in);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to load test properties", e);
        }
        return properties;
    }

    private static DataSource makeDataSource(Properties properties, String prefix) {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(properties.getProperty(prefix + "driver.classname"));
        ds.setUrl(properties.getProperty(prefix + "jdbc.url"));
        ds.setUsername(properties.getProperty(prefix + "jdbc.username"));
        ds.setPassword(properties.getProperty(prefix + "jdbc.password"));
        ds.setValidationQuery(properties.getProperty(prefix + "validation.query"));
        return ds;
    }

    public static DataSource makeTestDataSource() {
        final Properties properties = loadProperties();
        return makeDataSource(properties, "");
    }

    public static List<DataSource> makeSlaveTestDataSources() {
        final Properties properties = loadProperties();
        final String slavePropertyPrefixesStr = properties.getProperty("slave.property.prefixes");
        final String[] slavePropertyPrefixes =
                slavePropertyPrefixesStr != null? slavePropertyPrefixesStr.split(","): new String[0];
        final List<DataSource> result = new ArrayList<DataSource>(slavePropertyPrefixes.length);
        for (int i = 0; i < slavePropertyPrefixes.length; i++) {
            final String prefix = slavePropertyPrefixes[i].trim();
            result.add(makeDataSource(properties, prefix));
        }
        return result;
    }

    public static boolean isMysqlTestEnabled() {
        final Properties properties = loadProperties();
        final String status = properties.getProperty("test.mysql.support");
        return status != null && Boolean.parseBoolean(status);
    }

    public static void createTable(DataSource ds) {
        JdbcUtil.withConnectionNoResult(ds, new IConnectionActivityNoResult() {
            public void execute(Connection conn) {
                new DefaultJdbcWrite().update(conn, loadProperties().getProperty("create.table.ddl"), null);
            }
        });
    }

    public static void dropTable(DataSource ds) {
        JdbcUtil.withConnectionNoResult(ds, new IConnectionActivityNoResult() {
            public void execute(Connection conn) {
                new DefaultJdbcWrite().update(conn, loadProperties().getProperty("drop.table.ddl"), null);
            }
        });
    }

    public static void deleteAll(DataSourceTemplate dst) {
        dst.withConnectionNoResult(new IConnectionActivityNoResult() {
            public void execute(Connection conn) {
                new DefaultJdbcWrite().update(conn, "DELETE FROM session", null);
            }
        });
    }

    public static <K> long findRowCountForKeys(DataSourceTemplate dst, final List<K> keys) {
        return dst.withConnection(new IConnectionActivity<List<Long>>() {
            public List<Long> execute(Connection conn) {
                return new DefaultJdbcRead().queryForList(
                        conn, String.format("SELECT COUNT(*) FROM session WHERE skey IN (%s)",
                        JdbcUtil.paramPlaceholders(keys.size())), keys.toArray(),
                        JdbcUtil.makeColumnExtractor(Long.class, 1));
            }
        }).get(0).longValue();
    }


}
