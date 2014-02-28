# starfish

Java client library to emulate key-value store over JDBC backed database.

## Usage

_This library is in Alpha. Expected breaking changes._

### Maven Coordinates

Starfish is not on any public Maven repo yet, so you should run `mvn clean install` to install in the local repo.

Use the following Maven coordinates to include in your project.

```xml
  <dependencies>
    <dependency>
      <groupId>kumarshantanu</groupId>
      <artifactId>starfish</artifactId>
      <version>0.0.1-SNAPSHOT</version>
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
| Updated | Timestamp | NOT NULL    |

#### MySQL Example:

##### Minimal version:

```sql
CREATE TABLE session (
  id      VARCHAR(50) NOT NULL PRIMARY KEY,
  value   TEXT        NOT NULL,
  version BIGINT      NOT NULL,
  updated DATETIME    NOT NULL
) ENGINE=InnoDB;
```

##### Elaborate version

```sql
CREATE TABLE session (
  id      VARCHAR(50) NOT NULL PRIMARY KEY,
  value   TEXT        NOT NULL,
  version BIGINT      NOT NULL,
  updated DATETIME    NOT NULL,
  INDEX updated_index (updated) -- for faster search
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

import starfish.GenericOpsRead;
import starfish.GenericOpsWrite;
import starfish.IOpsRead;
import starfish.IOpsWrite;
import starfish.helper.ConnectionActivity;
import starfish.helper.JdbcUtil;
import starfish.type.TableMetadata;

public class SomeClass {
    final TableMetadata meta = TableMetadata.create("session", "id", "value", "version", "updated");
    final IOpsWrite<String, String> writer = new GenericOpsWrite<String, String>(meta);
    final IOpsRead<String, String> reader = new GenericOpsRead<String, String>(meta, String.class, String.class);

    public void savePair(DataSource ds) {
      final Long version = JdbcUtil.withConnection(ds, new ConnectionActivity<Long>() {
          public Long execute(Connection conn) {
              return writer.save(conn, "ABCD", "{\"email\": \"foo@bar.com\", \"age\": 29}");
          }
      });
      // do something with `version`...
    }

    public void readValue(DataSource ds) {
      final String value = JdbcUtil.withConnection(ds, new ConnectionActivity<String>() {
          public String execute(Connection conn) {
              return reader.read(conn, "ABCD");
          }
      });
      // do something with `value`...
    }
}
```

## License

Copyright © 2014 Shantanu Kumar

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
