package starfish.test;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import starfish.GenericOpsRead;
import starfish.IOpsRead;
import starfish.IOpsWrite;
import starfish.vendor.MysqlOpsWrite;

public class MysqlOpsTest {

    private static OpsTest opsTest;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        DataSource ds = TestUtil.makeTestDataSource();
        opsTest = new OpsTest(ds);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        opsTest = null;
    }

    @Before
    public void setUp() throws Exception {
        TestUtil.createTable(opsTest.ds);
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.dropTable(opsTest.ds);
    }

    @Test
    public void mysqlCrudTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlCrudTest()");
            final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, false);
            final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
            opsTest.crudTest(writer, reader);
        }
    }

    @Test
    public void mysqlCrudTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlCrudTestWithMysqlTimestamp()");
            final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, true);
            final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
            opsTest.crudTest(writer, reader);
        }
    }

    @Test
    public void mysqlVersionTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlVersionTest()");
            final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, false);
            final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
            opsTest.versionTest(writer, reader);
        }
    }

    @Test
    public void mysqlVersionTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlVersionTestWithMysqlTimestamp()");
            final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, true);
            final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
            opsTest.versionTest(writer, reader);
        }
    }

    @Test
    public void mysqlReadTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlReadTest()");
            final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, false);
            final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
            opsTest.readTest(writer, reader);
        }
    }

    @Test
    public void mysqlReadTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlReadTestWithMysqlTimestamp()");
            final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, true);
            final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
            opsTest.readTest(writer, reader);
        }
    }

}
