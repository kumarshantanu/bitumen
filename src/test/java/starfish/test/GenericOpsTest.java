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

    private static OpsSingleTest opsTest;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        DataSource ds = TestUtil.makeTestDataSource();
        opsTest = new OpsSingleTest(ds);
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
    public void genericCrudTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsTest.crudTest(writer, reader);
    }

    @Test
    public void genericVersionTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsTest.versionTest(writer, reader);
    }

    @Test
    public void genericReadTest() {
        final IOpsWrite<Integer, String> writer = new GenericOpsWrite<Integer, String>(TestUtil.meta);
        final IOpsRead<Integer, String> reader = new GenericOpsRead<Integer, String>(TestUtil.meta, Integer.class, String.class);
        opsTest.readTest(writer, reader);
    }

}
