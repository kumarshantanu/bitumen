package starfish.type;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

import starfish.helper.Util;

public class TableMetadata implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String tableName, keyColname, valueColname, versionColname, timestampColname;

    public TableMetadata(Map<String, String> names) {
        this.tableName = Util.notNull(names.get("tableName"), "tableName must not be null");
        this.keyColname = Util.notNull(names.get("keyColname"), "keyColname must not be null");
        this.valueColname = Util.notNull(names.get("valueColname"), "valueColname must not be null");
        this.versionColname = Util.notNull(names.get("versionColname"), "versionColname must not be null");
        this.timestampColname = Util.notNull(names.get("timestampColname"), "timestampColname must not be null");
    }

    public static TableMetadata create(String tableName) {
        Map<String, String> names = new HashMap<String, String>();
        names.put("tableName", tableName);
        names.put("keyColname", "key");
        names.put("valueColname", "value");
        names.put("versionColname", "version");
        names.put("timestampColname", "updated");
        return new TableMetadata(names);
    }

    public static TableMetadata create(String tableName, String keyColname, String valueColname, String versionColname,
            String timestampColname) {
        Map<String, String> names = new HashMap<String, String>();
        names.put("tableName", tableName);
        names.put("keyColname", keyColname);
        names.put("valueColname", valueColname);
        names.put("versionColname", versionColname);
        names.put("timestampColname", timestampColname);
        return new TableMetadata(names);
    }

    public String groovyReplace(String format) {
        return Util.groovyReplace(format, toMap(), true);
    }

    public String groovyReplaceKeep(String format) {
        return Util.groovyReplace(format, toMap(), false);
    }

    public Map<String, String> toMap() {
        Map<String, String> result = new HashMap<String, String>();
        result.put("tableName", tableName);
        result.put("keyColname", keyColname);
        result.put("valueColname", valueColname);
        result.put("versionColname", versionColname);
        result.put("timestampColname", timestampColname);
        return result;
    }

    @Override
    public String toString() {
        return String.format("tableName=%s, keyColname=%s, valueColname=%s, versionColname=%s, timestampColname=%s",
                tableName, keyColname, valueColname, versionColname, timestampColname);
    }

    @Override
    public int hashCode() {
        return (tableName + '|' + keyColname + '|' + valueColname + '|' + versionColname + '|' + timestampColname)
                .toLowerCase().hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || !(obj instanceof TableMetadata)) {
            return false;
        }
        TableMetadata that = (TableMetadata) obj;
        return tableName.equalsIgnoreCase(that.tableName) &&
                keyColname.equalsIgnoreCase(that.keyColname) &&
                valueColname.equalsIgnoreCase(that.valueColname) &&
                versionColname.equalsIgnoreCase(that.versionColname) &&
                timestampColname.equalsIgnoreCase(that.timestampColname);
    }

}
