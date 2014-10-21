package springer.jdbc.impl;

import java.sql.Connection;

public interface IConnectionActivity<V> {

    V execute(Connection conn);

}
