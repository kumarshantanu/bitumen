package net.sf.bitumen.test.jdbc;

import javax.sql.DataSource;

import net.sf.bitumen.jdbc.kv.IKeyvalRead;
import net.sf.bitumen.jdbc.kv.IKeyvalWrite;
import net.sf.bitumen.jdbc.kv.impl.DefaultKeyvalRead;
import net.sf.bitumen.jdbc.kv.impl.DefaultKeyvalWrite;
import net.sf.bitumen.test.helper.KeyvalTestBatch;
import net.sf.bitumen.test.helper.KeyvalTestSingle;
import net.sf.bitumen.test.helper.TestUtil;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DefaultKeyvalTest {

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
    final IKeyvalRead<Integer, String> reader = new DefaultKeyvalRead<Integer, String>(TestUtil.meta, Integer.class, String.class);

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
