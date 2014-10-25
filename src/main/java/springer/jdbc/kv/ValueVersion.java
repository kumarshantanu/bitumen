package springer.jdbc.kv;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import springer.jdbc.IRowExtractor;
import springer.jdbc.impl.JdbcUtil;
import springer.util.Util;

/**
 * Bean class to hold value and version.
 *
 * @param <V> value type
 */
public class ValueVersion<V> implements Serializable {

    /**
     * Class version; {@link Serializable} requires it.
     */
    private static final long serialVersionUID = 1L;

    /**
     * Stored value.
     */
    public final V value;

    /**
     * Stored version.
     */
    public final Long version;

    /**
     * Construct instance from value and version.
     * @param  theValue   the value
     * @param  theVersion the version
     */
    public ValueVersion(final V theValue, final Long theVersion) {
        this.value = theValue;
        this.version = theVersion;
    }

    @Override
    public final String toString() {
        return String.format("value=%s, version=%d", value, version);
    }

    @Override
    public final int hashCode() {
        final String compositeString = "" + value + '|' + version;
        return compositeString.hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null || !(obj instanceof ValueVersion)) {
            return false;
        }
        if (this == obj) {
            return true;
        }
        @SuppressWarnings("unchecked")
        ValueVersion<V> that = (ValueVersion<V>) obj;
        return Util.equals(value, that.value) && Util.equals(version, that.version);
    }

    /**
     * Create a row extractor from specified value and version meta data.
     * @param  <V>                value type
     * @param  valueClass         value class
     * @param  valueColumnIndex   value column index in result set
     * @param  versionColumnIndex version column index in result set
     * @return                    row extractor
     */
    public static <V> IRowExtractor<ValueVersion<V>> makeExtractor(final Class<V> valueClass,
            final int valueColumnIndex, final int versionColumnIndex) {
        return new IRowExtractor<ValueVersion<V>>() {
            public ValueVersion<V> extract(final ResultSet rs) {
                try {
                    return new ValueVersion<V>(valueClass.cast(JdbcUtil.getValue(rs, valueColumnIndex)),
                            rs.getLong(versionColumnIndex));
                } catch (SQLException e) {
                    throw new IllegalStateException("Unable to extract value and version", e);
                }
            }
        };
    }
}
