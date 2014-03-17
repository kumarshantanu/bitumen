package springer.test;

import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import springer.KeyvalRead;
import springer.KeyvalWrite;
import springer.impl.DefaultKeyvalWrite;
import springer.impl.ReplicatedKeyvalRead;
import springer.impl.ReplicationSlavesPointer;
import springer.test.helper.KeyvalTestBatch;
import springer.test.helper.KeyvalTestSingle;
import springer.test.helper.TestUtil;

public class ReplicatedKeyvalTest {

    private static DataSource dataSource;
    private static KeyvalTestSingle opsTestSingle;
    private static KeyvalTestBatch  opsTestBatch;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        dataSource = TestUtil.makeTestDataSource();
        opsTestSingle = new KeyvalTestSingle(dataSource);
        opsTestBatch = new KeyvalTestBatch(dataSource);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        opsTestSingle = null;
        opsTestBatch = null;
        dataSource = null;
    }

    final KeyvalWrite<Integer, String> writer = new DefaultKeyvalWrite<Integer, String>(TestUtil.meta);
    final List<DataSource> slaveDataSources = TestUtil.makeSlaveTestDataSources();
    final KeyvalRead<Integer, String> reader = new ReplicatedKeyvalRead<Integer, String>(TestUtil.meta, Integer.class, String.class, new ReplicationSlavesPointer() {
        public List<DataSource> getDataSources() {
            return slaveDataSources;
        }
    });

    @Before
    public void setUp() throws Exception {
        TestUtil.createTable(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.dropTable(dataSource);
    }

    @Test
    public void replicatedCrudTest() {
        opsTestSingle.crudTest(writer, reader);
    }

    @Test
    public void replicatedBatchCrudTest() {
        opsTestBatch.crudTest(writer, reader);
    }

    @Test
    public void replicatedVersionTest() {
        opsTestSingle.versionTest(writer, reader);
    }

    @Test
    public void replicatedBatchVersionTest() {
        opsTestBatch.versionTest(writer, reader);
    }

    @Test
    public void replicatedReadTest() {
        opsTestSingle.readTest(writer, reader);
    }

    @Test
    public void replicatedBatchReadTest() {
        opsTestBatch.readTest(writer, reader);
    }

}
