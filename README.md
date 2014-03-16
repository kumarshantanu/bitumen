# starfish

Java JDBC library to emulate key-value store over JDBC backed database.

Designed for Java 8 and beyond, compatible with Java 5 and upward.

## Usage

_This library is in Alpha. Expect breaking changes._

### Maven Coordinates

Starfish is not on any public Maven repo yet, so you should run `mvn clean install` to install in the local repo.

Use the following Maven coordinates to include in your project.

```xml
  <dependencies>
    <dependency>
      <groupId>kumarshantanu</groupId>
      <artifactId>starfish</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

### Pre-requisite

Starfish works with tables of an expected structure. You are free to choose the column names and type of the key and
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

### Operations

Use [Apache DBCP](http://commons.apache.org/proper/commons-dbcp/), [BoneCP](http://jolbox.com/) or a suitable library
to create a [javax.sql.DataSource](http://docs.oracle.com/javase/7/docs/api/javax/sql/DataSource.html) instance. Then
you can use it as follows:

```java
import java.sql.Connection;

import javax.sql.DataSource;

import starfish.KeyvalRead;
import starfish.KeyvalWrite;
import starfish.helper.ConnectionActivity;
import starfish.helper.DataSourceTemplate;
import starfish.impl.DefaultKeyvalRead;
import starfish.impl.DefaultKeyvalWrite;
import starfish.type.TableMetadata;

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

## License

Copyright © 2014 Shantanu Kumar

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
