package net.sf.bitumen.jdbc.instrument;

import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.logging.Logger;

import javax.sql.DataSource;

import net.sf.bitumen.util.IFactory;
import net.sf.bitumen.util.ILatencyLogger;
import net.sf.bitumen.util.timer.IStopWatch;

public class DataSourceWrapper implements DataSource {

    private final DataSource ds;
    private final ILatencyLogger<LatencyEventStatementCreation> stmtLatencyLogger;
    private final ILatencyLogger<LatencyEventSQLExecution> sqlLatencyLogger;
    private final IFactory<IStopWatch> stopWatchFactory;

    public DataSourceWrapper(DataSource ds, final ILatencyLogger<LatencyEventStatementCreation> stmtLatencyLogger,
            final ILatencyLogger<LatencyEventSQLExecution> sqlLatencyLogger,
            final IFactory<IStopWatch> stopWatchFactory) {
        this.ds = ds;
        this.stmtLatencyLogger = stmtLatencyLogger;
        this.sqlLatencyLogger = sqlLatencyLogger;
        this.stopWatchFactory = stopWatchFactory;
    }

    @Override
    public PrintWriter getLogWriter() throws SQLException {
        return ds.getLogWriter();
    }

    @Override
    public void setLogWriter(PrintWriter out) throws SQLException {
        ds.setLogWriter(out);
    }

    @Override
    public void setLoginTimeout(int seconds) throws SQLException {
        ds.setLoginTimeout(seconds);
    }

    @Override
    public int getLoginTimeout() throws SQLException {
        return ds.getLoginTimeout();
    }

    @Override
    public Logger getParentLogger() throws SQLFeatureNotSupportedException {
        return ds.getParentLogger();
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return ds.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return ds.isWrapperFor(iface);
    }

    @Override
    public Connection getConnection() throws SQLException {
        return new ConnectionWrapper(ds.getConnection(), stmtLatencyLogger, sqlLatencyLogger, stopWatchFactory);
    }

    @Override
    public Connection getConnection(String username, String password) throws SQLException {
        return new ConnectionWrapper(ds.getConnection(username, password), stmtLatencyLogger, sqlLatencyLogger,
                stopWatchFactory);
    }

}
