package springer.test.helper;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.junit.Assert;

import springer.JdbcException;
import springer.KeyvalRead;
import springer.KeyvalWrite;
import springer.helper.ConnectionActivity;
import springer.helper.ConnectionActivityNoResult;
import springer.helper.DataSourceTemplate;
import springer.helper.Util;
import springer.type.ValueVersion;

public class KeyvalTestBatch implements KeyvalTestSuite {

    public final DataSourceTemplate dst;

    public KeyvalTestBatch(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    private List<String> readValues(final KeyvalRead<Integer, String> reader, final List<Integer> keys) {
        final Map<Integer, String> kvPairs = dst.withConnection(new ConnectionActivity<Map<Integer, String>>() {
            public Map<Integer, String> execute(Connection conn) {
                return reader.batchRead(conn, keys);
            }
        });
        final List<String> result = new ArrayList<String>(keys.size());
        for (Integer key: keys) {
            result.add(kvPairs.get(key));
        }
        return result;
    }

    public void insertTest(final KeyvalWrite<Integer, String> writer, final KeyvalRead<Integer, String> reader) {
        // write (actually insert, because the value doesn't exist) key-value pairs
        final List<Integer> keys = Arrays.asList(1, 2, 3);
        final List<String> vals1 = Arrays.asList("abc", "bcd", "cde");
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchInsert(conn, Util.zipmap(keys, vals1));
            }
        });
        Assert.assertNotNull(version1);

        // read value for given key
        Assert.assertEquals(vals1, readValues(reader, keys));

        // make sure database table has the value
        Assert.assertEquals(3, TestUtil.findRowCountForKeys(dst, keys));

        // attempt duplicate insert, which should fail
        boolean exception = false;
        try {
            dst.withConnection(new ConnectionActivity<Long>() {
                public Long execute(Connection conn) {
                    return writer.batchInsert(conn, Util.zipmap(keys, vals1));
                }
            });
        } catch(JdbcException e) {
            exception = true;
        }
        Assert.assertTrue(exception);
    }

    public void crudTest(final KeyvalWrite<Integer, String> writer, KeyvalRead<Integer, String> reader) {
        // ----- INSERT (SAVE) -----

        // write (actually insert, because the value doesn't exist) key-value pairs
        final List<Integer> keys = Arrays.asList(1, 2, 3);
        final List<String> vals1 = Arrays.asList("abc", "bcd", "cde");
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSave(conn, Util.zipmap(keys, vals1));
            }
        });
        Assert.assertNotNull(version1);

        // read value for given key
        Assert.assertEquals(vals1, readValues(reader, keys));

        // make sure database table has the value
        Assert.assertEquals(3, TestUtil.findRowCountForKeys(dst, keys));

        // ----- UPDATE (SAVE) -----

        // write again (it is an update this time)
        final List<String> vals2 = Arrays.asList("vwx", "wxy", "xyz");
        final Long version2 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSave(conn, Util.zipmap(keys, vals2));
            }
        });
        Assert.assertNotNull(version2);
        Assert.assertNotEquals(version1, version2);

        // read value again for given key
        Assert.assertEquals(vals2, readValues(reader, keys));

        // make sure database table has the value
        Assert.assertEquals(3, TestUtil.findRowCountForKeys(dst, keys));

        // ----- UPDATE (SAVE) + INSERT -----

        // write again
        final List<Integer> keys2 = Arrays.asList(1, 2, 3, 4);
        final List<String> vals3 = Arrays.asList("pqr", "qrs", "rst", "stu");
        final Long version3 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSave(conn, Util.zipmap(keys2, vals3));
            }
        });
        Assert.assertNotNull(version3);
        Assert.assertNotEquals(version2, version3);

        // read value again for given key
        Assert.assertEquals(vals3, readValues(reader, keys2));

        // make sure database table has the value
        Assert.assertEquals(4, TestUtil.findRowCountForKeys(dst, keys2));

        // ----- DELETE -----

        // delete key-value pairs
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            public void execute(Connection conn) {
                writer.batchDelete(conn, keys);
            }
        });

        // make sure database table has the value
        Assert.assertEquals(0, TestUtil.findRowCountForKeys(dst, keys));
        Assert.assertEquals(1, TestUtil.findRowCountForKeys(dst, keys2));

        // read value again
        for (String each: readValues(reader, keys)) {
            Assert.assertNull(each);
        }
    }

    public void versionTest(final KeyvalWrite<Integer, String> writer, KeyvalRead<Integer, String> reader) {
        // save (insert)
        final List<Integer> keys = Arrays.asList(2, 3, 4);
        final List<String> vals1 = Arrays.asList("abc", "bcd", "cde");
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSave(conn, Util.zipmap(keys, vals1));
            }
        });
        Assert.assertNotNull(version1);

        // ----- SWAP -----

        // swap using invalid version, which should fail
        final List<String> vals2 = Arrays.asList("pqr", "qrs", "rst");
        final Long version2 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSwap(conn, Util.zipmap(keys, vals2), Util.newVersion());
            }
        });
        Assert.assertNull(version2);
        Assert.assertEquals(vals1, readValues(reader, keys));

        // swap using valid version, which should succeed
        final List<String> vals3 = Arrays.asList("uvw", "vwx", "wxy");
        final Long version3 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSwap(conn, Util.zipmap(keys, vals3), version1);
            }
        });
        Assert.assertNotNull(version3);
        Assert.assertNotEquals(version2, version3);
        Assert.assertEquals(vals3, readValues(reader, keys));

        // ----- REMOVE -----

        // remove with wrong version, which should fail
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            public void execute(Connection conn) {
                writer.batchRemove(conn, keys, version1);
            }
        });
        Assert.assertEquals(vals3, readValues(reader, keys));

        // remove with correct version, which should pass
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            public void execute(Connection conn) {
                writer.batchRemove(conn, keys, version3);
            }
        });
        for (String each: readValues(reader, keys)) {
            Assert.assertNull(each);
        }
    }

    public void readTest(final KeyvalWrite<Integer, String> writer, final KeyvalRead<Integer, String> reader) {
        final List<Integer> keys = Arrays.asList(1, 2, 3);

        // save (insert)
        final List<String> vals1 = Arrays.asList("abc", "bcd", "cde");
        final Long version1 = dst.withConnection(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.batchSave(conn, Util.zipmap(keys, vals1));
            }
        });
        Assert.assertNotNull(version1);

        // contains (which returns null due to bad key)
        for (Long each : dst.withConnection(new ConnectionActivity<List<Long>>() {
                    public List<Long> execute(Connection conn) {
                        return reader.batchContains(conn, Arrays.asList(Integer.MAX_VALUE));
                    }
                })) {
            Assert.assertNull(each);
        }

        // contains (which returns valid version)
        for (Long each : dst.withConnection(new ConnectionActivity<List<Long>>() {
                    public List<Long> execute(Connection conn) {
                        final List<Long> result = reader.batchContains(conn, keys);
                        return result;
                    }
                })) {
            Assert.assertEquals(version1, each);
        }

        // containsVersion (which returns null due to bad key)
        for (Boolean each : dst.withConnection(new ConnectionActivity<Map<Integer, Boolean>>() {
                    public Map<Integer, Boolean> execute(Connection conn) {
                        return reader.batchContainsVersion(conn, Collections.singletonMap(Integer.MAX_VALUE,
                                        Util.newVersion()));
                    }
                }).values()) {
            Assert.assertFalse(each);
        }

        // containsVersion (which passes valid version)
        for (Boolean each : dst.withConnection(new ConnectionActivity<Map<Integer, Boolean>>() {
                    public Map<Integer, Boolean> execute(Connection conn) {
                        return reader.batchContainsVersion(conn,
                                Util.zipmap(keys, Arrays.asList(version1, version1, version1)));
                    }
                }).values()) {
            Assert.assertTrue(each);
        }

        // read (which returns null due to bad key)
        for (String each: readValues(reader, Arrays.asList(Integer.MAX_VALUE))) {
            Assert.assertNull(each);
        }

        // read (which returns valid value)
        Assert.assertEquals(vals1, readValues(reader, keys));

        // readForVersion (which returns null due to bad version)
        for (String each : dst.withConnection(new ConnectionActivity<Map<Integer, String>>() {
                    public Map<Integer, String> execute(Connection conn) {
                        return reader.batchReadForVersion(conn,
                                Util.zipmap(keys,
                                        Arrays.asList(Util.newVersion(), Util.newVersion(), Util.newVersion())));
                    }
                }).values()) {
            Assert.assertNull(each);
        }

        // readForVersion (which returns correct value due to correct version)
        final Map<Integer, String> kvPairs = dst.withConnection(new ConnectionActivity<Map<Integer, String>>() {
            public Map<Integer, String> execute(Connection conn) {
                return reader.batchReadForVersion(conn, Util.zipmap(keys, Arrays.asList(version1, version1, version1)));
            }
        });
        Assert.assertEquals(vals1, Util.getVals(kvPairs, keys));

        // readAll
        final Map<Integer, ValueVersion<String>> kvv = dst
                .withConnection(new ConnectionActivity<Map<Integer, ValueVersion<String>>>() {
                    public Map<Integer, ValueVersion<String>> execute(
                            Connection conn) {
                        return reader.batchReadAll(conn, keys);
                    }
                });
        final List<ValueVersion<String>> vv = Util.getVals(kvv, keys);
        final List<String> vs = new ArrayList<String>(vv.size());
        final List<Long> vers = new ArrayList<Long>(vv.size());
        for (ValueVersion<String> each: vv) {
            vs.add(each.value);
            vers.add(each.version);
        }
        Assert.assertEquals(vals1, vs);
        Assert.assertEquals(Arrays.asList(version1, version1, version1), vers);
    }

}
