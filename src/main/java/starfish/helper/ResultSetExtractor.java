package starfish.helper;

import java.sql.ResultSet;

public interface ResultSetExtractor<T> {

    public T extract(ResultSet rs);

}
