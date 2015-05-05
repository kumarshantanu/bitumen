package net.sf.bitumen.jdbc.instrument;

public class LatencyEventStatementCreation {

    private final String sql;
    private final StatementType statementType;

    public LatencyEventStatementCreation(final String sql, final StatementType statementType) {
        this.sql = sql;
        this.statementType = statementType;
    }

    public String getSql() {
        return sql;
    }

    public StatementType getStatementType() {
        return statementType;
    }

    public static LatencyEventStatementCreation forStatement() {
        return new LatencyEventStatementCreation(null, StatementType.STATEMENT);
    }

    public static LatencyEventStatementCreation forPreparedStatement(final String sql) {
        return new LatencyEventStatementCreation(sql, StatementType.PREPARED_STATEMENT);
    }

    public static LatencyEventStatementCreation forCallableStatement(final String sql) {
        return new LatencyEventStatementCreation(sql, StatementType.PREPARED_CALL);
    }

}
