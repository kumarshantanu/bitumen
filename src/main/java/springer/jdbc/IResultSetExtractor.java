package springer.jdbc;

import java.sql.ResultSet;

public interface IResultSetExtractor<T> {

    public T extract(ResultSet rs);

}
