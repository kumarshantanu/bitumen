package starfish.test;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import starfish.GenericOpsRead;
import starfish.GenericOpsWrite;
import starfish.IOpsRead;
import starfish.IOpsWrite;
import starfish.test.helper.OpsTestBatch;
import starfish.test.helper.OpsTestSingle;
import starfish.test.helper.TestUtil;

public class GenericOpsTest {

    private static DataSource dataSource;
    private static OpsTestSingle opsTestSingle;
    private static OpsTestBatch  opsTestBatch;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        dataSource = TestUtil.makeTestDataSource();
        opsTestSingle = new OpsTestSingle(dataSource);
        opsTestBatch = new OpsTestBatch(dataSource);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        opsTestSingle = null;
        opsTestBatch = null;
    }

    final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
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
    public void genericInsertTest() {
        opsTestSingle.insertTest(writer, reader);
    }

    @Test
    public void genericBatchInsertTest() {
        opsTestBatch.insertTest(writer, reader);
    }

    @Test
    public void genericCrudTest() {
        opsTestSingle.crudTest(writer, reader);
    }

    @Test
    public void genericBatchCrudTest() {
        opsTestBatch.crudTest(writer, reader);
    }

    @Test
    public void genericVersionTest() {
        opsTestSingle.versionTest(writer, reader);
    }

    @Test
    public void genericBatchVersionTest() {
        opsTestBatch.versionTest(writer, reader);
    }

    @Test
    public void genericReadTest() {
        opsTestSingle.readTest(writer, reader);
    }

    @Test
    public void genericBatchReadTest() {
        opsTestBatch.readTest(writer, reader);
    }

}
