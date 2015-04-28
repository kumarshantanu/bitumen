package net.sf.bitumen.jdbc;

import java.sql.SQLException;

/**
 * Unchecked wrapper exception for {@link SQLException}.
 *
 */
public class JdbcException extends RuntimeException {

    /**
     * Class version, because some wise person decided that {@link Throwable} should be {@link Serializable}.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Construct instance from message and {@link SQLException} instance.
     * @param  msg error message
     * @param  e   {@link SQLException} instance
     */
    public JdbcException(final String msg, final SQLException e) {
        super(msg, e);
    }

}
