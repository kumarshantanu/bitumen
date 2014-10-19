package springer.jdbc.kv;

import java.io.Serializable;
import java.sql.ResultSet;
import java.sql.SQLException;

import springer.jdbc.RowExtractor;
import springer.jdbc.helper.JdbcUtil;
import springer.util.Util;

public class ValueVersion<V> implements Serializable {

    private static final long serialVersionUID = 1L;

    public final V value;
    public final Long version;

    public ValueVersion(final V value, final Long version) {
        this.value = value;
        this.version = version;
    }

    @Override
    public String toString() {
        return String.format("value=%s, version=%d", value, version);
    }

    @Override
    public int hashCode() {
        return ("" + value + '|' + version).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) return false;
        if (this == obj) return true;
        if (!(obj instanceof ValueVersion)) return false;
        @SuppressWarnings("unchecked")
        ValueVersion<V> that = (ValueVersion<V>) obj;
        return Util.equals(value, that.value) &&
                Util.equals(version, that.version);
    }

    public static <V> RowExtractor<ValueVersion<V>> makeExtractor(final Class<V> valueClass, final int valueColumnIndex,
            final int versionColumnIndex) {
        return new RowExtractor<ValueVersion<V>>() {
            
            public ValueVersion<V> extract(ResultSet rs) {
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
