package com.db.exporter.utils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.db.exporter.config.Configuration;

/**
 * This class is a wrapper of {@link Connection}. This class has a method for
 * creating database connection using configurations details provided by
 * {@link Configuration}
 * 
 * @author Abhijeet
 * 
 */
public class DBConnection {

	private Configuration configuration;
	static Log logger = LogFactory.getLog(DBConnection.class);

	/**
	 * This method is responsible for creating {@link Connection}
	 * 
	 * @return
	 */
	public Connection createConnection() {
		String url = StringUtils.getDerbyUrl(configuration.getDerbyDbPath(),
				configuration.getUserName(), configuration.getPassword());
		Connection connection = null;
		try {
			Class.forName(configuration.getDriverName()).newInstance();
			connection = DriverManager.getConnection(url);
		} catch (Exception e) {
			logger.error(e.getMessage(), e);
		}
		return connection;
	}

	public void shutdown() {
		try {
			DriverManager.getConnection("jdbc:derby:"+configuration.getDerbyDbPath()+";shutdown=true");
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * @param configuration
	 *            the configuration to set
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
