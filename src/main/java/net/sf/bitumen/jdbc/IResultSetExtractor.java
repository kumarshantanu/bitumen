package net.sf.bitumen.jdbc;

import java.sql.ResultSet;

/**
 * Functional interface to extract result from {@link ResultSet}. Broadest kind of extractor supported in
 * {@link IJdbcRead}.
 *
 * @param <T> type of result to be returned
 */
public interface IResultSetExtractor<T> {

    /**
     * Extract result from given {@link ResultSet}, without closing it. Keep no reference to the {@link ResultSet} in
     * captured result, e.g. no lazy reading of rows.
     * @param  rs the {@link ResultSet} instance
     * @return    result
     */
    T extract(ResultSet rs);

}
