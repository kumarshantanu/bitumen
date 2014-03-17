package springer.test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import javax.sql.DataSource;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import springer.JdbcRead;
import springer.JdbcWrite;
import springer.RowExtractor;
import springer.helper.ConnectionActivity;
import springer.helper.ConnectionActivityNoResult;
import springer.helper.DataSourceTemplate;
import springer.helper.Util;
import springer.impl.DefaultJdbcRead;
import springer.impl.DefaultJdbcWrite;
import springer.test.helper.TestUtil;
import springer.type.SqlParams;

public class DefaultJdbcTest {

    private static DataSource dataSource;
    private static DataSourceTemplate dst;

    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        dataSource = TestUtil.makeTestDataSource();
        dst = new DataSourceTemplate(dataSource);
    }

    @AfterClass
    public static void tearDownAfterClass() throws Exception {
        dst = null;
        dataSource = null;
    }

    @Before
    public void setUp() throws Exception {
        TestUtil.createTable(dataSource);
    }

    @After
    public void tearDown() throws Exception {
        TestUtil.dropTable(dataSource);
    }

    final JdbcRead reader = new DefaultJdbcRead();
    final JdbcWrite writer = new DefaultJdbcWrite();

    public static class Session {
        public int id;
        public int skey;
        public String value;
        public long version;
        public Timestamp created;
        public Timestamp updated;

        public Session setValue(String newValue) {
            final Session s = new Session();
            s.id = id;
            s.skey = skey;
            s.value = value;
            s.version = version;
            s.created = created;
            s.updated = updated;
            return s;
        }

        @Override
        public String toString() {
            return "" + id + '|' + skey + '|' +  value + '|' +  version + '|' +  created + '|' +  updated;
        }

        @Override
        public int hashCode() {
            return toString().hashCode();
        }

        public boolean equals(Object obj) {
            if (obj == null || !(obj instanceof Session)) {
                return false;
            }
            final Session that = (Session) obj;
            return Util.equals(skey, that.skey) && Util.equals(value, that.value) && Util.equals(version, that.version)
                    && Util.equals(created, that.created) && Util.equals(updated, that.updated);
        };
    }

    public static final RowExtractor<Session> sessionExtractor = new RowExtractor<Session>() {
        @Override
        public Session extract(ResultSet rs) throws SQLException {
            final Session session = new Session();
            session.skey = rs.getInt("skey");
            session.value = rs.getString("value");
            session.version = rs.getLong("version");
            session.created = rs.getTimestamp("created");
            session.updated = rs.getTimestamp("updated");
            return session;
        }
    };

    private Session readSession(Connection conn, int id) {
        final List<Session> slist = reader.queryForList(conn,
                "SELECT skey, value, version, created, updated FROM session WHERE id = ?", new Object[] { id },
                sessionExtractor);
        return slist.isEmpty()? null: slist.get(0);
    }

    private final Session s1 = new Session() {
        public Session init() {
            Timestamp now = Util.now();
            skey = 1001;
            value = "abc";
            version = Util.newVersion();
            created = now;
            updated = now;
            return this;
        }
    }.init();

    private final Session s2 = s1.setValue("xyz");

    @Test
    public void positionalParamsCrudTest() {
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            @Override
            public void execute(Connection conn) {
                // insert
                Integer id = (Integer) writer.genkey(conn,
                        "INSERT INTO session (skey, value, version, created, updated) VALUES (?, ?, ?, ?, ?)",
                        new Object[] {s1.skey, s1.value, s1.version, s1.created, s1.updated}).get();
                Assert.assertNotNull(id);
                Assert.assertEquals(s1, readSession(conn, id));  // read
                // update
                writer.update(conn, "UPDATE session SET value = ? WHERE id = ?", new Object[] { s2.value, id });
                Assert.assertEquals(s2, readSession(conn, id));  // read
                // delete
                writer.update(conn, "DELETE FROM session WHERE id = ?", new Object[] { id });
                Assert.assertNull(readSession(conn, id));
            }
        });
    }

    @Test
    public void namedParamsCrudTest() {
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            @Override
            public void execute(Connection conn) {
                // insert
                SqlParams insert = Util.namedParamReplace(
                        "INSERT INTO session (skey, value, version, created, updated) VALUES (:skey, :value, :version, :created, :updated)",
                        Util.makeParamMap("skey", s1.skey, "value", s1.value, "version", s1.version, "created", s1.created, "updated", s1.updated));
                Integer id = (Integer) insert.genkey(conn).get();
                Assert.assertNotNull(id);
                Assert.assertEquals(s1, readSession(conn, id));  // read
                // update
                SqlParams update = Util.namedParamReplace(
                        "UPDATE session SET value = :value WHERE id = :id",
                        Util.makeParamMap("value", s2.value, "id", id));
                update.update(conn);
                Assert.assertEquals(s2, readSession(conn, id));  // read
                // delete
                SqlParams delete = Util.namedParamReplace(
                        "DELETE FROM session WHERE id = :id",
                        Collections.singletonMap("id", (Object) id));
                delete.update(conn);
                Assert.assertNull(readSession(conn, id));
            }
        });
    }

    @Test
    public void transactionTest() {
        // commit with one update
        final int id = dst.withTransaction(new ConnectionActivity<Integer>() {
            @Override
            public Integer execute(Connection conn) {
                final Integer id = (Integer) writer.genkey(conn,
                        "INSERT INTO session (skey, value, version, created, updated) VALUES (?, ?, ?, ?, ?)",
                        new Object[] {s1.skey, s1.value, s1.version, s1.created, s1.updated}).get();
                Assert.assertNotNull(id);
                return id;
            }
        });
        dst.withConnectionNoResult(new ConnectionActivityNoResult() {
            @Override
            public void execute(Connection conn) {
                Assert.assertEquals(s1, readSession(conn, id));  // read
                TestUtil.deleteAll(dst);
            }
        });
        // rollback with two updates
        final boolean go = true;
        try {
            dst.withTransactionNoResult(new ConnectionActivityNoResult() {
                @Override
                public void execute(Connection conn) {
                    // TODO Auto-generated method stub
                    writer.update(conn,
                            "INSERT INTO session (skey, value, version, created, updated) VALUES (?, ?, ?, ?, ?)",
                            new Object[] {1001, s1.value, s1.version, s1.created, s1.updated});
                    if (go) throw new RuntimeException();
                    writer.update(conn,
                            "INSERT INTO session (skey, value, version, created, updated) VALUES (?, ?, ?, ?, ?)",
                            new Object[] {1002, s1.value, s1.version, s1.created, s1.updated});
                }
            });
        } catch (RuntimeException e) {
            // swallow exception
        }
        long rows = dst.withConnection(new ConnectionActivity<Long>() {
            @Override
            public Long execute(Connection conn) {
                return (Long) reader.queryForList(conn, "SELECT COUNT(*) FROM session", null).get(0)
                        .entrySet().iterator().next().getValue();
            }
        });
        Assert.assertEquals(0, rows);
    }

}
