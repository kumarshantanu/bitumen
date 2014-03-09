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
import starfish.test.helper.OpsTestBatch;
import starfish.test.helper.OpsTestSingle;
import starfish.test.helper.TestUtil;
import starfish.vendor.MysqlOpsWrite;

public class MysqlOpsTest {

    private static DataSource dataSource;
    private static OpsTestSingle opsTestSingle;
    private static OpsTestBatch  opsTestBatch;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        dataSource = TestUtil.makeTestDataSource();
        opsTestSingle = new OpsTestSingle(dataSource);
        opsTestBatch  = new OpsTestBatch(dataSource);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        dataSource = null;
        opsTestSingle = null;
        opsTestBatch  = null;
    }

    final IOpsWrite<Integer, String> writer = new MysqlOpsWrite<Integer, String>(TestUtil.meta, false);
    final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);

    @Before
    public void setUp() throws Exception {
        TestUtil.createTable(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.dropTable(dataSource);
    }

    @Test
    public void mysqlCrudTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlCrudTest()");
            opsTestSingle.crudTest(writer, reader);
        }
    }

    @Test
    public void mysqlBatchCrudTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlBatchCrudTest()");
            opsTestBatch.crudTest(writer, reader);
        }
    }

    @Test
    public void mysqlCrudTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlCrudTestWithMysqlTimestamp()");
            opsTestSingle.crudTest(writer, reader);
        }
    }

    @Test
    public void mysqlBatchCrudTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlCrudTestWithMysqlTimestamp()");
            opsTestBatch.crudTest(writer, reader);
        }
    }

    @Test
    public void mysqlVersionTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlVersionTest()");
            opsTestSingle.versionTest(writer, reader);
        }
    }

    @Test
    public void mysqlBatchVersionTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlBatchVersionTest()");
            opsTestBatch.versionTest(writer, reader);
        }
    }

    @Test
    public void mysqlVersionTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlVersionTestWithMysqlTimestamp()");
            opsTestSingle.versionTest(writer, reader);
        }
    }

    @Test
    public void mysqlBatchVersionTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlVersionTestWithMysqlTimestamp()");
            opsTestBatch.versionTest(writer, reader);
        }
    }

    @Test
    public void mysqlReadTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlReadTest()");
            opsTestSingle.readTest(writer, reader);
        }
    }

    @Test
    public void mysqlBatchReadTest() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlReadTest()");
            opsTestBatch.readTest(writer, reader);
        }
    }

    @Test
    public void mysqlReadTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlReadTestWithMysqlTimestamp()");
            opsTestSingle.readTest(writer, reader);
        }
    }

    @Test
    public void mysqlBatchReadTestWithMysqlTimestamp() {
        if (TestUtil.isMysqlTestEnabled()) {
            System.out.println("Running mysqlReadTestWithMysqlTimestamp()");
            opsTestBatch.readTest(writer, reader);
        }
    }

}
