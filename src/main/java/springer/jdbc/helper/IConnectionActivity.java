package springer.jdbc.helper;

import java.sql.Connection;

public interface IConnectionActivity<V> {

    public V execute(Connection conn);

}
