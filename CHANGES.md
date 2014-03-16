# Changes and TODO


## TODO / 0.2.0

* Optional indexing over map documents (support for synchronous and asynchronous)
* Reflection util to convert bean to/from map
* Serializer/deserializer interface to convert bean to/from EDN/JSON


## 2014-March-?? / 0.1.0

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
      * MySQL - save operation
* Sharding, Partitioning and Master/slave replication friendly API

