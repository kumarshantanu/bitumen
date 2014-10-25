package springer.jdbc.impl;

import java.sql.Connection;

/**
 * Functional interface to represent an activity that returns nothing.
 *
 */
public interface IConnectionActivityNoResult {

    /**
     * Body of the activity.
     * @param  conn {@link Connection} instance
     */
    void execute(Connection conn);

}
