package com.db.exporter.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import com.db.exporter.config.Configuration;

/**
 * This class is responsible for managing database connection. In this
 * application, there will be only one database connection(for Derby database).
 * 
 */
public class DBConnectionManager {

	private Connection connection;
	private static DBConnectionManager g_this;
	
	private DBConnectionManager(){
	}
	
	/**
	 * This method will return {@link Connection}.
	 * @return
	 * @throws SQLException 
	 */
	public static synchronized Connection getConnection() throws SQLException {
		if (g_this == null){
			g_this = new DBConnectionManager();
		}else if(g_this.createConnection() != null){
			/*Close existing connection*/
			g_this.connection.close();
		}
		
		g_this.connection = g_this.createConnection();
		
		return g_this.connection;
	}
	
	private Connection createConnection() throws SQLException {
		String url = StringUtils.getDerbyUrl(Configuration.getConfiguration().getDerbyDbPath(),
				Configuration.getConfiguration().getUserName(), Configuration.getConfiguration().getPassword());
		Connection connection = null;
		try {
			Class.forName(Configuration.getConfiguration().getDriverName()).newInstance();
			connection = DriverManager.getConnection(url);
		} catch (Exception e) {
			throw new SQLException(e.getCause());
		}
		return connection;
	}
}
