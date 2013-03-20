package com.db.exporter.utils;

import com.db.exporter.config.Configuration;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Singleton: Provides database connections. Note: Only provides one connection,
 * at the moment. Can be scaled to use a connection pool.
 * 
 */
public class DBConnectionManager {

	private Connection connection;
	private static DBConnectionManager g_this;

	private DBConnectionManager() {
	}

	/**
	 * @return Database connection
	 * @throws SQLException
	 */
	public static synchronized Connection getConnection(String url)
			throws SQLException {
		if (g_this == null) {
			g_this = new DBConnectionManager();
		} else if (g_this.connection != null) {
			/* Close existing connection */
			g_this.connection.close();
		}
		g_this.connection = g_this.createConnection(url);

		return g_this.connection;
	}
	
	private Connection createConnection(String url) throws SQLException {
		Connection connection = null;
		try {
			Class.forName(Configuration.getConfiguration().getDriverClassName())
					.newInstance();
			connection = DriverManager.getConnection(url);
		} catch (Exception e) {
			throw new SQLException(e.getCause());
		}
		return connection;
	}
}
