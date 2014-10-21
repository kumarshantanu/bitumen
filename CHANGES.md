# Changes and TODO


## TODO / 0.2.0

* Optional indexing over map documents (support for synchronous and asynchronous)
* Reflection util to convert bean to/from map, reflection/enum based RowMapper/RowExtractor
* Reflection based auto-generated RowMapper/RowExtractor
* Serializer/deserializer interface to convert bean to/from EDN/JSON
* Add @SafeVarargs to springer.di.DI.construct(..) when Java 7 is the minimum JDK version required
* Additional methods in IJdbcRead:
   * public <K> List<Map<K, Object>> queryForList(Connection conn, String sql, Object[] params, Class<K> clazz);
   * public <K> List<Map<K, Object>> queryForList(Connection conn, String sql, Object[] params, Class<K> clazz,
       long limit, boolean throwLimitExceedException);



## 2014-November-?? / 0.1.0

* Dependency Injection
   * Factory and method based
   * Constant and singleton support
   * Constructor based, auto-detected
* JDBC operations
   * Read operations
   * Write operations
   * Positional parameter support
   * Named parameter support
   * Transaction support
* Key-value storage operations
   * Write (individual and batch) support
      * insert
      * save
      * swap (optimistic locking)
      * touch (updates version - useful for locking)
      * delete
      * remove (delete with version check)
   * Read (individual and batch) support
      * check for existence (returns version)
      * read value (independent and version-based)
   * Compulsory version and create/update timestamp support
   * Read-consistency support for Master/slave replication
   * Fully customizable table column names
   * Restriction-free key and value types
   * Vendor-specific optimization
      * MySQL - save operation (UPSERT)
* Sharding, Partitioning and Master/slave replication friendly API

