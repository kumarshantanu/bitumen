package starfish;

import java.sql.SQLException;

public class JdbcException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public JdbcException(String msg, SQLException e) {
        super(msg, e);
    }

}
