package springer.jdbc;

import java.sql.ResultSet;

public interface ResultSetExtractor<T> {

    public T extract(ResultSet rs);

}
