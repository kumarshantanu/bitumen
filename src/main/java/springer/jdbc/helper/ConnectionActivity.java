package springer.jdbc.helper;

import java.sql.Connection;

public interface ConnectionActivity<V> {

    public V execute(Connection conn);

}
