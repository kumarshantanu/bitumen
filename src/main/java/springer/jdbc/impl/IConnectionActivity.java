package springer.jdbc.impl;

import java.sql.Connection;

/**
 * Functional interface to represent an activity to be done with {@link Connection}.
 *
 * @param  <V> return type of the activity
 */
public interface IConnectionActivity<V> {

    /**
     * Body of the activity.
     * @param  conn {@link Connection} instance
     * @return      activity result
     */
    V execute(Connection conn);

}
