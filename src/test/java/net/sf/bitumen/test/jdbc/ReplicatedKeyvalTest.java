package net.sf.bitumen.test.jdbc;

import java.util.List;

import javax.sql.DataSource;

import net.sf.bitumen.jdbc.kv.IKeyvalRead;
import net.sf.bitumen.jdbc.kv.IKeyvalWrite;
import net.sf.bitumen.jdbc.kv.impl.DefaultKeyvalWrite;
import net.sf.bitumen.jdbc.kv.impl.IReplicationSlavesPointer;
import net.sf.bitumen.jdbc.kv.impl.ReplicatedKeyvalRead;
import net.sf.bitumen.test.helper.KeyvalTestBatch;
import net.sf.bitumen.test.helper.KeyvalTestSingle;
import net.sf.bitumen.test.helper.TestUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

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

    final IKeyvalWrite<Integer, String> writer = new DefaultKeyvalWrite<Integer, String>(TestUtil.meta);
    final List<DataSource> slaveDataSources = TestUtil.makeSlaveTestDataSources();
    final IKeyvalRead<Integer, String> reader = new ReplicatedKeyvalRead<Integer, String>(TestUtil.meta, Integer.class, String.class, new IReplicationSlavesPointer() {
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
