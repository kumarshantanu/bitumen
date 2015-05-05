package net.sf.bitumen.jdbc.instrument;

public enum SQLStatementType {

    QUERY  ("SQL Query"),
    UPDATE ("SQL Update"),
    SQL    ("SQL (possibly DDL)");

    private final String strValue;

    private SQLStatementType(final String strValue) {
        this.strValue = strValue;
    }

    @Override
    public String toString() {
        return strValue;
    }

}
