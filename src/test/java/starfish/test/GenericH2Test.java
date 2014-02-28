package starfish.test;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import starfish.GenericOpsRead;
import starfish.GenericOpsWrite;
import starfish.IOpsRead;
import starfish.IOpsWrite;
import starfish.helper.ConnectionActivity;
import starfish.helper.JdbcUtil;
import starfish.type.TableMetadata;

public class GenericH2Test {

    DataSource ds = TestUtil.makeH2DataSource();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Test
    public void test() {
        final TableMetadata meta = TableMetadata.create("session", "id", "value", "version", "updated");
        final IOpsWrite<String, String> writer = new GenericOpsWrite<String, String>(meta);
        final Long version = JdbcUtil.withConnection(ds, new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, "id=1", "value=abc");
            }
        });
        final IOpsRead<String, String> reader = new GenericOpsRead<String, String>(meta, String.class, String.class);
        final String value = JdbcUtil.withConnection(ds, new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.read(conn, "id=1");
            }
        });
        List<Long> count = JdbcUtil.withConnection(ds, new ConnectionActivity<List<Long>>() {
            public List<Long> execute(Connection conn) {
                return JdbcUtil.queryVals(conn, "SELECT COUNT(*) FROM session", null,
                        JdbcUtil.makeColumnExtractor(Long.class, 1));
            }
        });
        Assert.assertEquals(0, count.get(0).longValue());
    }

}
