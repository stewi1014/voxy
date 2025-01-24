package me.cortex.voxy.common.storage.postgres;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PostgreSQLConnectionPool {
    final String databaseUrl;
    int connectionOff = 0;
    int totalConnections = 0;
    int waiters = 0;
    Connection[] connections;
    ReentrantReadWriteLock closeMutex = new ReentrantReadWriteLock();

    public PostgreSQLConnectionPool(String databaseUrl, int maxConnections) {
        this.databaseUrl = databaseUrl;
        connections = new Connection[maxConnections];
    }

    private synchronized Connection takeConnection() throws SQLException, InterruptedException {
        while (true) {
            if (connectionOff > 0) {
                return connections[connectionOff-- - 1];
            }

            if (totalConnections >= connections.length) {
                // all DB connections are currently in use,
                // and we're at the connection limit - can't open a new one.
                // So, we have to block until a connection is freed.
                waiters++;
                this.wait();
                waiters--;
                continue;
            }

            totalConnections++;
            try {
                return DriverManager.getConnection(databaseUrl);
            } catch (SQLException e) {
                totalConnections--;
                throw e;
            }
        }
    }

    private synchronized void returnConnection(Connection connection) {
        connections[connectionOff++] = connection;
        if (waiters > 0)
            this.notify();
    }

    @FunctionalInterface
    public interface Update {
        void runUpdate(Connection connection) throws SQLException;
    }

    public void withConnection(Update update) {
        withConnection(connection -> {
            update.runUpdate(connection);
            return null;
        });
    }

    @FunctionalInterface
    public interface Query<T> {
        T runQuery(Connection connection) throws SQLException;
    }

    public <T> T withConnection(Query<T> query) {
        closeMutex.readLock().lock();
        try {
            Connection connection;
            try {
                connection = takeConnection();
            } catch (SQLException | InterruptedException e) {
                throw new RuntimeException(e);
            }

            T value;
            try {
                value = query.runQuery(connection);
            } catch (SQLException e) {
                synchronized (this) {
                    totalConnections--;
                }
                throw new RuntimeException(e);
            }

            returnConnection(connection);
            return value;
        } finally {
            closeMutex.readLock().unlock();
        }
    }

    /**
     * close releases all resources associated with the ConnectionPool,
     * however the ConnectionPool can be used again. Connections will simply be recreated,
     * and another call to close will need to be made to free them.
     */
    public void close() {
        // we acquire the close mutex before, and release it after, entering the
        // synchronized block. This is because withConnection only briefly holds the object lock
        // while it pops and pushes connections from the pool - but not during the usage of them.
        // If close was synchronized and was called while withConnection was running its query,
        // close would acquire the object lock and wait on acquiring close mutex,
        // while withConnection holds the close mutex and waits on acquiring the object lock.
        // deadlock.
        this.closeMutex.writeLock().lock();
        try {
            synchronized (this) {
                while (this.connectionOff > 0) {
                    try {
                        connections[connectionOff-- - 1].close();
                    } catch (SQLException ignored) {
                    }
                }
                totalConnections = 0;
            }
        } finally {
            this.closeMutex.writeLock().unlock();
        }
    }
}
