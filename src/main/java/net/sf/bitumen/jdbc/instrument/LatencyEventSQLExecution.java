package net.sf.bitumen.jdbc.instrument;

public class LatencyEventSQLExecution {

    public static final int NOT_KNOWN = -1;  // updateCount

    private final boolean prepared;
    private final String sql;
    private final SQLStatementType statementType;
    private final int updateCount;

    public LatencyEventSQLExecution(final SQLStatementType statementType, final boolean prepared, final String sql,
            final int updateCount) {
        this.statementType = statementType;
        this.prepared = prepared;
        this.sql = sql;
        this.updateCount = updateCount;
    }

    public boolean isPrepared() {
        return prepared;
    }

    public String getSql() {
        return sql;
    }

    public SQLStatementType getStmtType() {
        return statementType;
    }

    public int getUpdateCount() {
        return updateCount;
    }

    public static LatencyEventSQLExecution forStatement(final SQLStatementType statementType, final String sql,
            final int updateCount) {
        return new LatencyEventSQLExecution(statementType, false, sql, updateCount);
    }

    public static LatencyEventSQLExecution forPreparedStatement(final SQLStatementType statementType, final String sql,
            final int updateCount) {
        return new LatencyEventSQLExecution(statementType, true, sql, updateCount);
    }

}
