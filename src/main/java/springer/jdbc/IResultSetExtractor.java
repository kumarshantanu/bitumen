package springer.jdbc;

import java.sql.ResultSet;

public interface IResultSetExtractor<T> {

    T extract(ResultSet rs);

}
