package starfish.helper;

import java.sql.ResultSet;

public interface RowExtractor<T> {

    public T extract(ResultSet rs);

}
