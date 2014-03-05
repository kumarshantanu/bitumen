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
import starfish.helper.ConnectionActivityWithoutResult;
import starfish.helper.DataSourceTemplate;
import starfish.helper.JdbcUtil;
import starfish.type.TableMetadata;

public class OpsTest {

    final DataSource ds = TestUtil.makeTestDataSource();
    final DataSourceTemplate dst = new DataSourceTemplate(ds);

    final TableMetadata meta = TableMetadata.create("session", "id", "value", "version", "updated");
    final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(meta);
    final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(meta, Integer.class, String.class);

    private <K> long findRowCountForKey(final K key) {
        return JdbcUtil.withConnection(ds, new ConnectionActivity<List<Long>>() {
            public List<Long> execute(Connection conn) {
                return JdbcUtil.queryVals(conn, "SELECT COUNT(*) FROM session WHERE ID = ?", new Object[] { key },
                        JdbcUtil.makeColumnExtractor(Long.class, 1));
            }
        }).get(0).longValue();
    }

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Test
    public void crudTest() {
        // ----- INSERT -----

        // write (actually insert, because the value doesn't exist) key-value pair
        final int key = 1;
        final String newValue1 = "abc";
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, key, newValue1);
            }
        });
        Assert.assertNotNull(version1);

        // read value for given key
        final String savedValue1 = dst.withConnection(new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.read(conn, key);
            }
        });
        Assert.assertEquals(newValue1, savedValue1);

        // make sure database table has the value
        Assert.assertEquals(1, findRowCountForKey(key));

        // ----- UPDATE -----

        // write again (it is an update this time)
        final String newValue2 = "xyz";
        final Long version2 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, key, newValue2);
            }
        });
        Assert.assertNotNull(version2);
        Assert.assertNotEquals(version1, version2);

        // read value again for given key
        final String savedValue2 = dst.withConnection(new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.read(conn, key);
            }
        });
        Assert.assertEquals(newValue2, savedValue2);

        // make sure database table has the value
        Assert.assertEquals(1, findRowCountForKey(key));

        // ----- DELETE -----

        // delete key-value pair
        dst.withConnectionWithoutResult(new ConnectionActivityWithoutResult() {
            public void execute(Connection conn) {
                writer.delete(conn, key);
            }
        });

        // read value again
        final String deletedValue = dst.withConnection(new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.read(conn, key);
            }
        });
        Assert.assertNull(deletedValue);

        // make sure database table has the value
        Assert.assertEquals(0, findRowCountForKey(key));
    }

}
