package net.sf.bitumen.jdbc;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Functional interface to extract a row in {@link ResultSet}.
 *
 * @param  <T> type of result
 */
public interface IRowExtractor<T> {

    /**
     * Extract a row from {@link ResultSet}, without closing it or changing its state.
     * @param  rs {@link ResultSet} instance
     * @return    result of extracting a row
     * @throws    SQLException thrown by operations related to {@link ResultSet} <tt>rs</tt>
     */
    T extract(ResultSet rs) throws SQLException;

}
