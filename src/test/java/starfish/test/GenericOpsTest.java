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

public class GenericOpsTest {

    private static DataSource dataSource;
    private static OpsTestSingle opsSingleTest;
    private static OpsTestBatch  opsBatchTest;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        dataSource = TestUtil.makeTestDataSource();
        opsSingleTest = new OpsTestSingle(dataSource);
        opsBatchTest = new OpsTestBatch(dataSource);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        opsSingleTest = null;
        opsBatchTest = null;
    }

    @Before
    public void setUp() throws Exception {
        TestUtil.createTable(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.dropTable(dataSource);
    }

    @Test
    public void genericCrudTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsSingleTest.crudTest(writer, reader);
    }

    @Test
    public void genericBatchCrudTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsBatchTest.crudTest(writer, reader);
    }

    @Test
    public void genericVersionTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsSingleTest.versionTest(writer, reader);
    }

    @Test
    public void genericBatchVersionTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsBatchTest.versionTest(writer, reader);
    }

    @Test
    public void genericReadTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsSingleTest.readTest(writer, reader);
    }

    @Test
    public void genericBatchReadTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsBatchTest.readTest(writer, reader);
    }

}
