package net.sf.bitumen.jdbc.instrument;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.Statement;

import net.sf.bitumen.util.IFactory;
import net.sf.bitumen.util.ILatencyLogger;
import net.sf.bitumen.util.timer.IStopWatch;

public class StatementWrapper implements Statement {

    private final Connection conn;
    private final Statement stmt;
    private final ILatencyLogger<LatencyEventSQLExecution> sqlLatencyLogger;
    private final IFactory<IStopWatch> stopWatchFactory;

    public StatementWrapper(final Connection conn, final Statement stmt,
            final ILatencyLogger<LatencyEventSQLExecution> sqlLatencyLogger,
            final IFactory<IStopWatch> stopWatchFactory) {
        this.conn = conn;
        this.stmt = stmt;
        this.sqlLatencyLogger = sqlLatencyLogger;
        this.stopWatchFactory = stopWatchFactory;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return stmt.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return stmt.isWrapperFor(iface);
    }

    @Override
    public ResultSet executeQuery(String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("Expected valid SQL, but found NULL");
        }
        final IStopWatch timer = stopWatchFactory.createInstance();
        try {
            return stmt.executeQuery(sql);
        } finally {
            final long duration = timer.elapsed();
            int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
            try {
                updateCount = stmt.getUpdateCount();
            } finally {
                sqlLatencyLogger.logLatency(duration,
                        LatencyEventSQLExecution.forStatement(SQLStatementType.QUERY, sql, updateCount));
            }
        }
    }

    @Override
    public int executeUpdate(String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("Expected valid SQL, but found NULL");
        }
        final IStopWatch timer = stopWatchFactory.createInstance();
        int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
        try {
            updateCount = stmt.executeUpdate(sql);
            return updateCount;
        } finally {
            sqlLatencyLogger.logLatency(timer.elapsed(),
                    LatencyEventSQLExecution.forStatement(SQLStatementType.UPDATE, sql, updateCount));
        }
    }

    @Override
    public void close() throws SQLException {
        stmt.close();
    }

    @Override
    public int getMaxFieldSize() throws SQLException {
        return stmt.getMaxFieldSize();
    }

    @Override
    public void setMaxFieldSize(int max) throws SQLException {
        stmt.setMaxFieldSize(max);
    }

    @Override
    public int getMaxRows() throws SQLException {
        return stmt.getMaxRows();
    }

    @Override
    public void setMaxRows(int max) throws SQLException {
        stmt.setMaxRows(max);
    }

    @Override
    public void setEscapeProcessing(boolean enable) throws SQLException {
        stmt.setEscapeProcessing(enable);
    }

    @Override
    public int getQueryTimeout() throws SQLException {
        return stmt.getQueryTimeout();
    }

    @Override
    public void setQueryTimeout(int seconds) throws SQLException {
        stmt.setQueryTimeout(seconds);
    }

    @Override
    public void cancel() throws SQLException {
        stmt.cancel();
    }

    @Override
    public SQLWarning getWarnings() throws SQLException {
        return stmt.getWarnings();
    }

    @Override
    public void clearWarnings() throws SQLException {
        stmt.clearWarnings();
    }

    @Override
    public void setCursorName(String name) throws SQLException {
        stmt.setCursorName(name);
    }

    @Override
    public boolean execute(String sql) throws SQLException {
        if (sql == null) {
            throw new NullPointerException("Expected valid SQL, but found NULL");
        }
        final IStopWatch timer = stopWatchFactory.createInstance();
        try {
            return stmt.execute(sql);
        } finally {
            int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
            try {
                updateCount = stmt.getUpdateCount();
            } finally {
                sqlLatencyLogger.logLatency(timer.elapsed(),
                        LatencyEventSQLExecution.forStatement(SQLStatementType.SQL, sql, updateCount));
            }
        }
    }

    @Override
    public ResultSet getResultSet() throws SQLException {
        return stmt.getResultSet();
    }

    @Override
    public int getUpdateCount() throws SQLException {
        return stmt.getUpdateCount();
    }

    @Override
    public boolean getMoreResults() throws SQLException {
        return stmt.getMoreResults();
    }

    @Override
    public void setFetchDirection(int direction) throws SQLException {
        stmt.setFetchDirection(direction);
    }

    @Override
    public int getFetchDirection() throws SQLException {
        return stmt.getFetchDirection();
    }

    @Override
    public void setFetchSize(int rows) throws SQLException {
        stmt.setFetchSize(rows);
    }

    @Override
    public int getFetchSize() throws SQLException {
        return stmt.getFetchSize();
    }

    @Override
    public int getResultSetConcurrency() throws SQLException {
        return stmt.getResultSetConcurrency();
    }

    @Override
    public int getResultSetType() throws SQLException {
        return stmt.getResultSetType();
    }

    @Override
    public void addBatch(String sql) throws SQLException {
        stmt.addBatch(sql);
    }

    @Override
    public void clearBatch() throws SQLException {
        stmt.clearBatch();
    }

    @Override
    public int[] executeBatch() throws SQLException {
        return stmt.executeBatch();
    }

    @Override
    public Connection getConnection() throws SQLException {
        return conn;
    }

    @Override
    public boolean getMoreResults(int current) throws SQLException {
        return stmt.getMoreResults();
    }

    @Override
    public ResultSet getGeneratedKeys() throws SQLException {
        return stmt.getGeneratedKeys();
    }

    @Override
    public int executeUpdate(String sql, int autoGeneratedKeys) throws SQLException {
        final IStopWatch timer = stopWatchFactory.createInstance();
        int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
        try {
            updateCount = stmt.executeUpdate(sql, autoGeneratedKeys);
            return updateCount;
        } finally {
            sqlLatencyLogger.logLatency(timer.elapsed(),
                    LatencyEventSQLExecution.forStatement(SQLStatementType.UPDATE, sql, updateCount));
        }
    }

    @Override
    public int executeUpdate(String sql, int[] columnIndexes) throws SQLException {
        final IStopWatch timer = stopWatchFactory.createInstance();
        int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
        try {
            updateCount = stmt.executeUpdate(sql, columnIndexes);
            return updateCount;
        } finally {
            sqlLatencyLogger.logLatency(timer.elapsed(),
                    LatencyEventSQLExecution.forStatement(SQLStatementType.UPDATE, sql, updateCount));
        }
    }

    @Override
    public int executeUpdate(String sql, String[] columnNames) throws SQLException {
        final IStopWatch timer = stopWatchFactory.createInstance();
        int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
        try {
            updateCount = stmt.executeUpdate(sql, columnNames);
            return updateCount;
        } finally {
            sqlLatencyLogger.logLatency(timer.elapsed(),
                    LatencyEventSQLExecution.forStatement(SQLStatementType.UPDATE, sql, updateCount));
        }
    }

    @Override
    public boolean execute(String sql, int autoGeneratedKeys) throws SQLException {
        final IStopWatch timer = stopWatchFactory.createInstance();
        try {
            return stmt.execute(sql, autoGeneratedKeys);
        } finally {
            int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
            try {
                updateCount = stmt.getUpdateCount();
            } finally {
                sqlLatencyLogger.logLatency(timer.elapsed(),
                        LatencyEventSQLExecution.forStatement(SQLStatementType.SQL, sql, updateCount));
            }
        }
    }

    @Override
    public boolean execute(String sql, int[] columnIndexes) throws SQLException {
        final IStopWatch timer = stopWatchFactory.createInstance();
        try {
            return stmt.execute(sql, columnIndexes);
        } finally {
            int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
            try {
                updateCount = stmt.getUpdateCount();
            } finally {
                sqlLatencyLogger.logLatency(timer.elapsed(),
                        LatencyEventSQLExecution.forStatement(SQLStatementType.SQL, sql, updateCount));
            }
        }
    }

    @Override
    public boolean execute(String sql, String[] columnNames) throws SQLException {
        final IStopWatch timer = stopWatchFactory.createInstance();
        try {
            return stmt.execute(sql, columnNames);
        } finally {
            int updateCount = LatencyEventSQLExecution.NOT_KNOWN;
            try {
                updateCount = stmt.getUpdateCount();
            } finally {
                sqlLatencyLogger.logLatency(timer.elapsed(),
                        LatencyEventSQLExecution.forStatement(SQLStatementType.SQL, sql, updateCount));
            }
        }
    }

    @Override
    public int getResultSetHoldability() throws SQLException {
        return stmt.getResultSetHoldability();
    }

    @Override
    public boolean isClosed() throws SQLException {
        return stmt.isClosed();
    }

    @Override
    public void setPoolable(boolean poolable) throws SQLException {
        stmt.setPoolable(poolable);
    }

    @Override
    public boolean isPoolable() throws SQLException {
        return stmt.isPoolable();
    }

    @Override
    public void closeOnCompletion() throws SQLException {
        stmt.closeOnCompletion();
    }

    @Override
    public boolean isCloseOnCompletion() throws SQLException {
        return stmt.isCloseOnCompletion();
    }

}
