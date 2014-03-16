package starfish;

import java.sql.ResultSet;

public interface RowExtractor<T> {

    public T extract(ResultSet rs);

}
