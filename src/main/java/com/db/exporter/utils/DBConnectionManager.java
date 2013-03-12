package com.db.exporter.utils;

import java.sql.Connection;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is responsible for managing database connection. In this
 * application, there will be only one database connection(for Derby database).
 * 
 * @author Abhijeet
 * 
 */
public class DBConnectionManager {

	private DBConnection dbConnection;
	private Connection connection;
	static Log logger = LogFactory.getLog(DBConnectionManager.class);

	/**
	 * This method will return {@link Connection}.
	 * @return
	 */
	public Connection getConnection() {
		if (connection == null){
			logger.debug("connection object is null. Creating a new connection.");
			connection = dbConnection.createConnection();
		}
		return connection;
	}
	
	public boolean isClosed(){
		boolean flag = false;
		if (connection != null)
			try {
				flag = connection.isClosed();
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		return flag;
	}
	
	

	/**
	 * This method will disconnect the connection.
	 */
	public void disconnect() {
		if (!isClosed()){
			try {
				connection.close();
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
			dbConnection.shutdown();
		}
	}

	/**
	 * @return the dbConnection
	 */
	public DBConnection getDbConnection() {
		return dbConnection;
	}

	/**
	 * @param dbConnection
	 *            the dbConnection to set
	 */
	public void setDbConnection(DBConnection dbConnection) {
		this.dbConnection = dbConnection;
	}
}
