package starfish.test;

import java.sql.Connection;
import java.util.List;

import javax.sql.DataSource;

import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import starfish.helper.ConnectionActivity;
import starfish.helper.JdbcUtil;

public class GenericH2Test {

    DataSource ds = TestUtil.makeH2DataSource();

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
    }

    @Test
    public void test() {
        List<Long> count = JdbcUtil.withConnection(ds, new ConnectionActivity<List<Long>>() {
            public List<Long> execute(Connection conn) {
                return JdbcUtil.queryVals(conn, "SELECT COUNT(*) FROM session", null,
                        JdbcUtil.makeColumnExtractor(Long.class, 1));
            }
        });
        Assert.assertEquals(0, count.get(0).longValue());
    }

}
