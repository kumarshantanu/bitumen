package starfish.test.helper;

import java.sql.Connection;
import java.util.Arrays;

import javax.sql.DataSource;

import org.junit.Assert;

import starfish.IOpsRead;
import starfish.IOpsWrite;
import starfish.helper.ConnectionActivity;
import starfish.helper.ConnectionActivityNoResult;
import starfish.helper.DataSourceTemplate;
import starfish.helper.Util;
import starfish.type.ValueVersion;

public class OpsTestSingle implements OpsTestSuite {

    public final DataSourceTemplate dst;

    public OpsTestSingle(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    private String readValue(final IOpsRead<Integer, String> reader, final Integer key) {
        return dst.withConnection(new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.read(conn, key);
            }
        });
    }

    public void insertTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader) {
        final int key1 = 1;
        final String val1 = "abc";
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.insert(conn, key1, val1);
            }
        });
        Assert.assertNotNull(version1);

        // read value for given key
        Assert.assertEquals(val1, readValue(reader, key1));

        // make sure database table has the value
        Assert.assertEquals(1, TestUtil.findRowCountForKeys(dst, Arrays.asList(key1)));

        // attempt a duplicate insert which should fail
        boolean exception = false;
        try {
            dst.withConnection(new ConnectionActivity<Long>() {
                public Long execute(Connection conn) {
                    return writer.insert(conn, key1, val1);
                }
            });
        } catch(IllegalStateException e) {
            exception = true;
        }
        Assert.assertTrue(exception);
    }

    public void crudTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader) {
        // ----- INSERT (SAVE) -----

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
        Assert.assertEquals(newValue1, readValue(reader, key));

        // make sure database table has the value
        Assert.assertEquals(1, TestUtil.findRowCountForKeys(dst, Arrays.asList(key)));

        // ----- UPDATE (SAVE) -----

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
        Assert.assertEquals(newValue2, readValue(reader, key));

        // make sure database table has the value
        Assert.assertEquals(1, TestUtil.findRowCountForKeys(dst, Arrays.asList(key)));

        // ----- DELETE -----

        // delete key-value pair
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            public void execute(Connection conn) {
                writer.delete(conn, key);
            }
        });

        // read value again
        Assert.assertNull(readValue(reader, key));

        // make sure database table does not have the value
        Assert.assertEquals(0, TestUtil.findRowCountForKeys(dst, Arrays.asList(key)));
    }

    public void versionTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader) {
        // save (insert)
        final int key = 2;
        final String newValue1 = "abc";
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, key, newValue1);
            }
        });
        Assert.assertNotNull(version1);

        // ----- SWAP -----

        // swap using invalid version, which should fail
        final String newValue2 = "pqr";
        final Long version2 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.swap(conn, key, newValue2, Util.newVersion());
            }
        });
        Assert.assertNull(version2);
        Assert.assertEquals(newValue1, readValue(reader, key));

        // swap using valid version, which should succeed
        final String newValue3 = "pqr";
        final Long version3 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.swap(conn, key, newValue3, version1);
            }
        });
        Assert.assertNotNull(version3);
        Assert.assertNotEquals(version2, version3);
        Assert.assertEquals(newValue3, readValue(reader, key));

        // ----- REMOVE -----

        // remove with wrong version, which should fail
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            public void execute(Connection conn) {
                writer.remove(conn, key, version1);
            }
        });
        Assert.assertEquals(newValue3, readValue(reader, key));

        // remove with correct version, which should pass
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            public void execute(Connection conn) {
                writer.remove(conn, key, version3);
            }
        });
        Assert.assertNull(readValue(reader, key));
    }

    public void readTest(final IOpsWrite<Integer, String> writer, final IOpsRead<Integer, String> reader) {
        final int key = 3;

        // save (insert)
        final String newValue1 = "abc";
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, key, newValue1);
            }
        });
        Assert.assertNotNull(version1);

        // contains (which returns null due to bad key)
        Assert.assertNull(dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return reader.contains(conn, Integer.MAX_VALUE);
            }
        }));

        // contains (which returns valid version)
        Assert.assertEquals(version1, dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return reader.contains(conn, key);
            }
        }));

        // containsVersion (which returns null due to bad key)
        Assert.assertFalse(dst.withConnection(new ConnectionActivity<Boolean>() {
            public Boolean execute(Connection conn) {
                return reader.containsVersion(conn, Integer.MAX_VALUE, Util.newVersion());
            }
        }));

        // containsVersion (which passes valid version)
        Assert.assertTrue(dst.withConnection(new ConnectionActivity<Boolean>() {
            public Boolean execute(Connection conn) {
                return reader.containsVersion(conn, key, version1);
            }
        }));

        // read (which returns null due to bad key)
        Assert.assertNull(readValue(reader, Integer.MAX_VALUE));

        // read (which returns valid value)
        Assert.assertEquals(newValue1, readValue(reader, key));

        // readForVersion (which returns null due to bad version)
        Assert.assertNull(dst.withConnection(new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.readForVersion(conn, key, Util.newVersion());
            }
        }));

        // readForVersion (which returns correct value due to correct version)
        Assert.assertEquals(newValue1, dst.withConnection(new ConnectionActivity<String>() {
            public String execute(Connection conn) {
                return reader.readForVersion(conn, key, version1);
            }
        }));

        // readAll
        final ValueVersion<String> vv = dst.withConnection(new ConnectionActivity<ValueVersion<String>>() {
            public ValueVersion<String> execute(Connection conn) {
                return reader.readAll(conn, key);
            }
        });
        Assert.assertEquals(newValue1, vv.value);
        Assert.assertEquals(version1, vv.version);
    }

}
