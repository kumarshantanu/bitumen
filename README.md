# bitumen

A simple Java library (no XML, no annotations) to provide
* Dependency Injection (DI)
* JDBC access
* Key-value store over JDBC backed database

Designed for Java 8 and beyond, compatible with Java 7 and above.

## Usage

_This library is in Alpha. Expect breaking changes._

### Maven Coordinates

Bitumen is not on any public Maven repo yet, so you should run `mvn clean install` to install in the local repo.

Use the following Maven coordinates to include in your project.

```xml
  <dependencies>
    <dependency>
      <groupId>net.sf.bitumen</groupId>
      <artifactId>bitumen</artifactId>
      <version>0.1.0-SNAPSHOT</version>
    </dependency>
  </dependencies>
```

## Documentation

See the file `DOC.md` in this repo for documentation.

## License

Copyright © 2014-2015 Shantanu Kumar

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
