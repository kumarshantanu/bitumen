# springer v0.1.x documentation

Springer can be used to:

* Work with SQL databases
* Emulate key-value store

Springer uses [`javax.sql.DataSource`](http://download.java.net/jdk8/docs/api/javax/sql/DataSource.html) to obtain JDBC
connections. You can use [Apache DBCP](http://commons.apache.org/proper/commons-dbcp/), [BoneCP](http://jolbox.com/) or a
suitable library to create a [`javax.sql.DataSource`](http://docs.oracle.com/javase/7/docs/api/javax/sql/DataSource.html)
instance.

## Work with SQL databases

Springer supports interface-based API for flexibility, as well as fluent interface. It also supports transactions,
positional parameters and named parameters.

### Interface-based API

The following example shows the usage of interface based API.

```java
import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import springer.JdbcRead;
import springer.JdbcWrite;
import springer.helper.ConnectionActivityNoResult;
import springer.helper.DataSourceTemplate;
import springer.impl.DefaultJdbcRead;
import springer.impl.DefaultJdbcWrite;

public class JdbcExample {

    final JdbcRead  reader = new DefaultJdbcRead();
    final JdbcWrite writer = new DefaultJdbcWrite();

    final DataSourceTemplate dst;

    public JdbcExample(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    public void crud() {
        // Java 7 and below
        dst.withTransactionNoResult(new ConnectionActivityNoResult() {
            @Override
            public void execute(Connection conn) {
                // insert
                final long key = (Long) writer.genkey(conn,
                        "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                        new Object[] { "E-1196", "Joe Walker" }).get();
                // read
                Map<String, Object> row = reader.queryForList(conn,
                        "SELECT * FROM emp WHERE emp_id = ?",
                        new Object[] { "E-1196" }).get(0);
                // update
                int updates = writer.update(conn,
                        "UPDATE emp SET emp_name = ? WHERE emp_id = ?",
                        new Object[] { "Joe Nixon", "E-1196" });
                // delete
                int deletes = writer.update(conn,
                        "DELETE FROM emp WHERE emp_id = ?",
                        new Object[] { "E-1196" });
            }
        });

        // Java 8 and beyond
        dst.withTransactionNoResult(conn -> {
            // insert
            final long key = (Long) writer.genkey(conn,
                    "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                    new Object[] { "E-1196", "Joe Walker" }).get();
            // read
            Map<String, Object> row = reader.queryForList(conn,
                    "SELECT * FROM emp WHERE emp_id = ?",
                    new Object[] { "E-1196" }).get(0);
            // update
            int updates = writer.update(conn,
                    "UPDATE emp SET emp_name = ? WHERE emp_id = ?",
                    new Object[] { "Joe Nixon", "E-1196" });
            // delete
            int deletes = writer.update(conn,
                    "DELETE FROM emp WHERE emp_id = ?",
                    new Object[] { "E-1196" });
        });
    }

}
```

### Fluent-interface example

The following example shows the usage of fluent API and named parameter support.

```java
import java.sql.Connection;
import java.util.Collections;
import java.util.Map;

import javax.sql.DataSource;

import springer.helper.ConnectionActivityNoResult;
import springer.helper.DataSourceTemplate;
import springer.helper.Util;
import springer.type.SqlParams;

public class FluentExample {

    final DataSourceTemplate dst;

    public FluentExample(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    public void crud() {
        // Java 7 and below
        dst.withTransactionNoResult(new ConnectionActivityNoResult() {
            @Override
            public void execute(Connection conn) {
                // insert using positional parameters
                final long key = (Long) new SqlParams(
                        "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                        new Object[] { "E-1196", "Joe Walker" }).genkey(conn).get();
                // read using positional parameters
                Map<String, Object> row = new SqlParams(
                        "SELECT * FROM emp WHERE emp_id = ?",
                        new Object[] { "E-1196" }).queryForList(conn).get(0);
                // update using named parameters
                int updates = Util.namedParamReplace(
                        "UPDATE emp SET emp_name = :emp_name WHERE emp_id = :emp_id",
                        Util.makeParamMap("emp_name", "Joe Nixon", "emp_id", "E-1196"))
                        .update(conn);
                // delete using named parameters
                int deletes = Util.namedParamReplace(
                        "DELETE FROM emp WHERE emp_id = :emp_id",
                        Collections.singletonMap("emp_id", "E-1196"))
                        .update(conn);
            }
        });

        // Java 8 and beyond
        dst.withTransactionNoResult(conn -> {
            // insert using positional parameters
            final long key = (Long) new SqlParams(
                    "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                    new Object[] { "E-1196", "Joe Walker" }).genkey(conn).get();
            // read using positional parameters
            Map<String, Object> row = new SqlParams(
                    "SELECT * FROM emp WHERE emp_id = ?",
                    new Object[] { "E-1196" }).queryForList(conn).get(0);
            // update using named parameters
            int updates = Util.namedParamReplace(
                    "UPDATE emp SET emp_name = :emp_name WHERE emp_id = :emp_id",
                    Util.makeParamMap("emp_name", "Joe Nixon", "emp_id", "E-1196"))
                    .update(conn);
            // delete using named parameters
            int deletes = Util.namedParamReplace(
                    "DELETE FROM emp WHERE emp_id = :emp_id",
                    Collections.singletonMap("emp_id", "E-1196"))
                    .update(conn);
        });
    }
```

## Emulate key-value store

Springer can also help emulate key-value store over ordinary SQL databases. It is tested with H2, MySQL and PostgreSQL.

### Pre-requisite

Springer works with tables of an expected structure. You are free to choose the column names and type of the key and
value columns. The columns are:

| Column  | Java Type | Constraints |
|---------|-----------|-------------|
| Key     |    Any    | PRIMARY KEY |
| Value   |    Any    | NOT NULL    |
| Version |    Long   | NOT NULL    |
| Created | Timestamp | NOT NULL    |
| Updated | Timestamp | NOT NULL    |

##### Minimal MySQL example:

```sql
CREATE TABLE session (
  id      VARCHAR(50) NOT NULL PRIMARY KEY,
  value   TEXT        NOT NULL,
  version BIGINT      NOT NULL,
  created DATETIME    NOT NULL,
  updated DATETIME    NOT NULL
) ENGINE=InnoDB;
```

##### Elaborate MySQL example:

```sql
CREATE TABLE session (
  id      VARCHAR(50) NOT NULL PRIMARY KEY,
  value   TEXT        NOT NULL,
  version BIGINT      NOT NULL,
  created DATETIME    NOT NULL,
  updated DATETIME    NOT NULL,
  INDEX updated_index (updated) -- for faster manual search
) ENGINE=InnoDB
  PARTITION BY HASH( id ) -- for faster updates and individual reads
  PARTITIONS 10;
```

### Key-value Operations

Setup the `javax.sql.DataSource` and use it as follows:

```java
import java.sql.Connection;

import javax.sql.DataSource;

import springer.KeyvalRead;
import springer.KeyvalWrite;
import springer.helper.ConnectionActivity;
import springer.helper.DataSourceTemplate;
import springer.impl.DefaultKeyvalRead;
import springer.impl.DefaultKeyvalWrite;
import springer.type.TableMetadata;

public class Example {

    final TableMetadata meta = TableMetadata.create("session", "id", "value",
            "version", "created", "updated");
    final KeyvalWrite<String, String> writer = new DefaultKeyvalWrite<String, String>(
            meta);
    final KeyvalRead<String, String> reader = new DefaultKeyvalRead<String, String>(
            meta, String.class, String.class);
    final DataSourceTemplate dst;

    public Example(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    public void savePair() {
        final Long version = dst.withTransaction(new ConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, "ABCD",
                        "{\"email\": \"foo@bar.com\", \"age\": 29}");
            }
        });
        // do something with `version`...
    }

    public void readValue() {
      final String value = dst.withConnection(new ConnectionActivity<String>() {
          public String execute(Connection conn) {
              return reader.read(conn, "ABCD");
          }
      });
      // do something with `value`...
    }

    // ----- Java 8 examples -----

    public void savePairJava8() {
        final Long version = dst.withTransaction(conn -> writer.save(conn,
                "ABCD", "{\"email\": \"foo@bar.com\", \"age\": 29}"));
        // do something with `version`...
    }

    public void readValueJava8() {
        final String value = dst.withConnection(conn -> reader.read(conn, "ABCD"));
        // do something with `value`...
    }

}
```
