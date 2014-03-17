package springer;

import java.sql.ResultSet;
import java.sql.SQLException;

public interface RowExtractor<T> {

    public T extract(ResultSet rs) throws SQLException;

}
