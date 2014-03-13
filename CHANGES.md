# Changes and TODO


## TODO / 0.2.0

* Optional indexing over map documents (support for synchronous and asynchronous)
* Reflection util to convert bean to/from map
* Serializer/deserializer interface to convert bean to/from EDN/JSON


## 2014-March-?? / 0.1.0

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
* Master/slave replication aware
* Fully customizable table column names
* Restriction-free key and value types
* Vendor-specific optimization
   * MySQL - save operation
* Sharding and Partitioning friendly API

