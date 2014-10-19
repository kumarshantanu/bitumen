# springer v0.1.x documentation

Springer can be used to:

* Accomplish Dependency Injection without XML/annotations
* Work with SQL databases using JDBC
* Emulate key-value store over JDBC

Springer uses [`javax.sql.DataSource`](http://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html) to obtain JDBC
connections. You can use [Apache DBCP](http://commons.apache.org/proper/commons-dbcp/), [BoneCP](http://jolbox.com/) or a
suitable library to create a [`javax.sql.DataSource`](http://docs.oracle.com/javase/8/docs/api/javax/sql/DataSource.html)
instance.

## Dependency Injection

Springer provides a simple, programmatic API for dependency injection. Implicit setter-based injection is not supported.
Constructor based injection is supported. Consider the Java 8 example below:

```java
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

import springer.di.DI;
import springer.di.DependencyBuilder;
import springer.di.IComponentSource;
import springer.di.IDependencyBuilder;
import springer.di.PropertyGetter;
import springer.example.support.DefaultBizService;
import springer.example.support.DefaultComplexService;
import springer.example.support.DefaultDataAccess;
import springer.example.support.DefaultEmailer;
import springer.example.support.DummyDataSource;
import springer.example.support.IBizService;
import springer.example.support.IComplexService;
import springer.example.support.IDataAccess;
import springer.example.support.IEmailer;

public class DIExample {

    public enum Bean {
        APP_PROPERTIES    (Properties.class),
        DUMMY_DATA_SOURCE (DummyDataSource.class),
        EMAIL_SERVICE     (IEmailer.class),
        DATA_ACCESS       (IDataAccess.class),
        COMPLEX_SERVICE   (IComplexService.class),
        BIZ_SERVICE       (IBizService.class);

        public final Class<?> type;

        private Bean() { this.type = Object.class; }
        private Bean(Class<?> type) { this.type = type; }

        public static Map<Bean, Class<?>> getTypes() {
            final Bean[] values = Bean.values();
            final Map<Bean, Class<?>> types = new LinkedHashMap<>();
            for (Bean each: values) {
                types.put(each, each.type);
            }
            return types;
        }
    }

    @SuppressWarnings("unchecked")
    public void init(Properties appConfig) {
        final PropertyGetter<Bean> pg = new PropertyGetter<>(appConfig, Bean.class);
        final Map<Bean, Class<?>> types = Bean.getTypes();
        final IDependencyBuilder<Bean> db = new DependencyBuilder<Bean>();
        db
        // adding constant
        .addConstant(Bean.APP_PROPERTIES, appConfig)

        // add constructor based on java.util.Property values
        .addSingleton(Bean.DUMMY_DATA_SOURCE, DI.construct(DummyDataSource.class,
                pg.getString("db.jdbc.url"), pg.getString("db.username"), pg.getString("db.password")))

        // add constructor based on component keys
        .addSingleton(Bean.DATA_ACCESS, DI.constructByKey(DefaultDataAccess.class, Bean.DUMMY_DATA_SOURCE))

        // add constructor based on both component keys and constants
        .addSingleton(Bean.EMAIL_SERVICE, DI.construct(DefaultEmailer.class,
                DI.sourceOf(Bean.DATA_ACCESS), pg.getString("smtp.host"), pg.getInteger("smtp.port")))

        // add factory method
        .addSingleton(Bean.COMPLEX_SERVICE, this::createComplexService)

        // add auto-detecting constructor
        .addSingleton(Bean.BIZ_SERVICE, DI.autoConstruct(types, DefaultBizService.class))
        .getDependencyMap();

        // verify everything works
        Properties ps = db.getInstance(Bean.APP_PROPERTIES, Properties.class);
        DummyDataSource ds = db.getInstance(Bean.DUMMY_DATA_SOURCE, DummyDataSource.class);
        IDataAccess da = db.getInstance(Bean.DATA_ACCESS, IDataAccess.class);
        IEmailer em = db.getInstance(Bean.EMAIL_SERVICE, IEmailer.class);
        IBizService bs = db.getInstance(Bean.BIZ_SERVICE, IBizService.class);
        if (bs == null) {
            throw new RuntimeException("Could not instantiate BizService");
        } else {
            System.out.println("All well");
        }
    }

    IComplexService createComplexService(Map<Bean, IComponentSource<?, Bean>> beans) {
        Properties appConfig = DI.getInstance(beans, Bean.APP_PROPERTIES, Properties.class);
        Object initState = appConfig.get("complex.init.state");
        DefaultComplexService cs = new DefaultComplexService();
        cs.setStarted(initState==null? false: Boolean.parseBoolean((String) initState));
        return cs;
    }

    public static void main(String[] args) {
        Properties appConfig = new Properties();
        appConfig.setProperty("db.jdbc.url", "jdbc:mysql://localhost:3306/test");
        appConfig.setProperty("db.username", "user");
        appConfig.setProperty("db.password", "pass");
        appConfig.setProperty("smtp.host", "localhost");
        appConfig.setProperty("smtp.port", "25");
        new DIExample().init(appConfig);
    }
}
```

## Work with SQL databases using JDBC

Springer supports interface-based API for flexibility, as well as fluent interface. It also supports transactions,
positional parameters and named parameters.

### Interface-based API

The following example shows the usage of interface based API.

```java
import java.sql.Connection;
import java.util.Map;

import javax.sql.DataSource;

import springer.jdbc.IJdbcRead;
import springer.jdbc.IJdbcWrite;
import springer.jdbc.impl.DataSourceTemplate;
import springer.jdbc.impl.DefaultJdbcRead;
import springer.jdbc.impl.DefaultJdbcWrite;
import springer.jdbc.impl.IConnectionActivityNoResult;

public class JdbcExample {

    final IJdbcRead  reader = new DefaultJdbcRead();
    final IJdbcWrite writer = new DefaultJdbcWrite();

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
                final long key = writer.genkey(conn,
                        "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                        new Object[] { "E-1196", "Joe Walker" }).get().longValue();
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
            final long key = writer.genkey(conn,
                    "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                    new Object[] { "E-1196", "Joe Walker" }).get().longValue();
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

import springer.jdbc.impl.DataSourceTemplate;
import springer.jdbc.impl.IConnectionActivityNoResult;
import springer.jdbc.impl.JdbcUtil;
import springer.jdbc.impl.SqlParams;
import springer.util.Util;

public class FluentExample {

    final DataSourceTemplate dst;

    public FluentExample(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    public void crud() {
        // Java 7 and below
        dst.withTransactionNoResult(new IConnectionActivityNoResult() {
            @Override
            public void execute(Connection conn) {
                // insert using positional parameters
                final long key = new SqlParams(
                        "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                        new Object[] { "E-1196", "Joe Walker" }).genkey(conn).get().longValue();
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
            final long key = new SqlParams(
                    "INSERT INTO emp (emp_id, emp_name) VALUES (?, ?)",
                    new Object[] { "E-1196", "Joe Walker" }).genkey(conn).get().longValue();
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

## Emulate key-value store over JDBC

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

import springer.jdbc.impl.IConnectionActivity;
import springer.jdbc.impl.DataSourceTemplate;
import springer.jdbc.kv.IKeyvalRead;
import springer.jdbc.kv.IKeyvalWrite;
import springer.jdbc.kv.impl.DefaultKeyvalRead;
import springer.jdbc.kv.impl.DefaultKeyvalWrite;
import springer.jdbc.type.TableMetadata;

public class Example {

    final TableMetadata meta = TableMetadata.create("session", "id", "value",
            "version", "created", "updated");
    final IKeyvalWrite<String, String> writer = new DefaultKeyvalWrite<String, String>(
            meta);
    final IKeyvalRead<String, String> reader = new DefaultKeyvalRead<String, String>(
            meta, String.class, String.class);
    final DataSourceTemplate dst;

    public Example(DataSource ds) {
        this.dst = new DataSourceTemplate(ds);
    }

    public void savePair() {
        final Long version = dst.withTransaction(new IConnectionActivity<Long>() {
            public Long execute(Connection conn) {
                return writer.save(conn, "ABCD",
                        "{\"email\": \"foo@bar.com\", \"age\": 29}");
            }
        });
        // do something with `version`...
    }

    public void readValue() {
      final String value = dst.withConnection(new IConnectionActivity<String>() {
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
