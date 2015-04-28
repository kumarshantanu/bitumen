package net.sf.bitumen.jdbc.kv.impl;

import java.util.List;

import javax.sql.DataSource;

/**
 * Functional interface to represent a source of slave {@link DataSource} instances.
 *
 */
public interface IReplicationSlavesPointer {

    /**
     * Obtain a list of slave {@link DataSource} instances.
     * @return list of slave {@link DataSource} instances
     */
    List<DataSource> getDataSources();

}
