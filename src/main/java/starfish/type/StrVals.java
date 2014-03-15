package starfish.type;

import java.io.Serializable;
import java.util.Arrays;

import starfish.helper.Util;

public class StrVals implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String str;
    public final Object[] vals;

    public StrVals(String str, Object[] vals) {
        this.str = str;
        this.vals = vals;
    }

    @Override
    public String toString() {
        return String.format("str = %s, vals = %s", str, Arrays.toString(vals));
    }

    @Override
    public int hashCode() {
        return ("" + str + '|' + Arrays.toString(vals)).hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof StrVals)) {
            return false;
        }
        final StrVals that = (StrVals) obj;
        return Util.equals(str, that.str) && Util.equals(vals, that.vals);
    }

}
