package springer.jdbc.kv.impl;

import java.util.List;

import javax.sql.DataSource;

public interface IReplicationSlavesPointer {

    public List<DataSource> getDataSources();

}
