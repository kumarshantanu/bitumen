package springer.jdbc.kv.impl;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import springer.util.Util;

/**
 * Table meta data for a key-value store.
 *
 */
public class TableMetadata implements Serializable {

    /** Class version; {@link Serializable} requires it. */
    private static final long serialVersionUID = 1L;

    /** Meta data attributes. */
    private final String tableName, keyColname, valueColname, versionColname,
    createTimestampColname, updateTimestampColname;

    /**
     * Getter for tableName.
     * @return tableName
     */
    public final String getTableName() {
        return tableName;
    }

    /**
     * Getter for keyColname.
     * @return keyColname
     */
    public final String getKeyColname() {
        return keyColname;
    }

    /**
     * Getter for valueColname.
     * @return valueColname
     */
    public final String getValueColname() {
        return valueColname;
    }

    /**
     * Getter for versionColname.
     * @return versionColname
     */
    public final String getVersionColname() {
        return versionColname;
    }

    /**
     * Getter for createTimestampColname.
     * @return createTimestampColname
     */
    public final String getCreateTimestampColname() {
        return createTimestampColname;
    }

    /**
     * Getter for updateTimestampColname.
     * @return updateTimestampColname
     */
    public final String getUpdateTimestampColname() {
        return updateTimestampColname;
    }

    /**
     * Construct instance from arguments passed as a map.
     * @param names map of argument names and values
     */
    public TableMetadata(final Map<String, String> names) {
        this.tableName = Util.notNull(names.get("tableName"), "tableName must not be null");
        this.keyColname = Util.notNull(names.get("keyColname"), "keyColname must not be null");
        this.valueColname = Util.notNull(names.get("valueColname"), "valueColname must not be null");
        this.versionColname = Util.notNull(names.get("versionColname"), "versionColname must not be null");
        this.createTimestampColname = Util.notNull(names.get("createTimestampColname"),
                "createTimestampColname must not be null");
        this.updateTimestampColname = Util.notNull(names.get("updateTimestampColname"),
                "updateTimestampColname must not be null");
    }

    /**
     * Factory method to create new instance based on table name and default values for other arguments.
     * @param  tableName table name
     * @return           table meta data instance
     */
    public static TableMetadata create(final String tableName) {
        Map<String, String> names = new HashMap<String, String>();
        names.put("tableName", tableName);
        names.put("keyColname", "key");
        names.put("valueColname", "value");
        names.put("versionColname", "version");
        names.put("createTimestampColname", "created");
        names.put("updateTimestampColname", "updated");
        return new TableMetadata(names);
    }

    /**
     * Factory method to create instance from all required arguments.
     * @param  tableName              table name
     * @param  keyColname             column name for <i>key</i>
     * @param  valueColname           column name for <i>value</i>
     * @param  versionColname         column name for <i>version</i>
     * @param  createTimestampColname column name for <i>created timestamp</i>
     * @param  updateTimestampColname column name for <i>last updated timestamp</i>
     * @return                        table meta data instance
     */
    public static TableMetadata create(final String tableName, final String keyColname, final String valueColname,
            final String versionColname, final String createTimestampColname, final String updateTimestampColname) {
        final Map<String, String> names = new HashMap<String, String>();
        names.put("tableName", tableName);
        names.put("keyColname", keyColname);
        names.put("valueColname", valueColname);
        names.put("versionColname", versionColname);
        names.put("createTimestampColname", createTimestampColname);
        names.put("updateTimestampColname", updateTimestampColname);
        return new TableMetadata(names);
    }

    /**
     * Like string variable replacement feature in <a href="http://groovy-lang.org/">Groovy language</a>, replace table
     * meta data variables with corresponding values. If a remaining variable exists, that may cause
     * {@link RuntimeException} to be thrown.
     * @param  format the format string, containing variables
     * @return        string wherein table meta data variables are replaced by corresponding values, and any remaining
     *                variable causes {@link RuntimeException}
     */
    public final String groovyReplace(final String format) {
        return Util.groovyReplace(format, toMap(), true);
    }

    /**
     * Potentially partial variable replacement for table meta data variables. No exception thrown if unresolved
     * variables found.
     * @param  format string format containing variables
     * @return        string wherein table meta data variables are replaced by corresponding values, and remaining
     *                variables left intact
     * @see           #groovyReplace(String)
     */
    public final String groovyReplaceKeep(final String format) {
        return Util.groovyReplace(format, toMap(), false);
    }

    /**
     * Turn table meta data into a map of attribute names and values.
     * @return map of attribute names and values
     */
    public final Map<String, String> toMap() {
        final Map<String, String> result = new HashMap<String, String>();
        result.put("tableName", tableName);
        result.put("keyColname", keyColname);
        result.put("valueColname", valueColname);
        result.put("versionColname", versionColname);
        result.put("updateTimestampColname", updateTimestampColname);
        result.put("createTimestampColname", createTimestampColname);
        return result;
    }

    @Override
    public final String toString() {
        return String.format(
                "tableName=%s, keyColname=%s, valueColname=%s, versionColname=%s,"
                + " createTimestampColname=%s, updateTimestampColname=%s",
                tableName, keyColname, valueColname, versionColname, createTimestampColname, updateTimestampColname);
    }

    @Override
    public final int hashCode() {
        final String compositeString = tableName + '|' + keyColname + '|' + valueColname + '|' + versionColname + '|'
                + createTimestampColname + '|' + updateTimestampColname;
        return compositeString.toLowerCase().hashCode();
    }

    @Override
    public final boolean equals(final Object obj) {
        if (obj == null || !(obj instanceof TableMetadata)) {
            return false;
        }
        final TableMetadata that = (TableMetadata) obj;
        return tableName.equalsIgnoreCase(that.tableName)
                && keyColname.equalsIgnoreCase(that.keyColname)
                && valueColname.equalsIgnoreCase(that.valueColname)
                && versionColname.equalsIgnoreCase(that.versionColname)
                && createTimestampColname.equalsIgnoreCase(that.createTimestampColname)
                && updateTimestampColname.equalsIgnoreCase(that.updateTimestampColname);
    }

}
