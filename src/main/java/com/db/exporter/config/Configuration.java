package com.db.exporter.config;

import java.util.Properties;

/**
 * Loads relevant application settings from properties file, by default.
 * 
 */
public class Configuration {

	private static Configuration m_this;

	private innerConfig m_default;

	private Configuration() {
		m_default = new innerConfig();
	}

	public static synchronized Configuration getConfiguration() {
		if (m_this == null) {
			m_this = new Configuration();
		}
		return m_this;
	}

	public static synchronized Configuration getConfiguration(String userName, String password, String derbyPath, String driverName, String schema, int maxBufferSize, String dumpLocation) {
		if (m_this == null) {
			m_this = new Configuration();
		}
		m_this.m_default = m_this.new innerConfig(userName, password, derbyPath, driverName, schema, maxBufferSize, dumpLocation);
		
		return m_this;
	}
	
	/**
	 * @return Database userName
	 */
	public String getUserName() {
		return m_default.m_userName;
	}

	/**
	 * @return Database password
	 */
	public String getPassword() {
		return m_default.m_password;
	}

	/**
	 * @return Absolute path to the database
	 */
	public String getDerbyDbPath() {
		return m_default.m_derbyDbPath;
	}

	/**
	 * @return JDBC driver name
	 */
	public String getDriverName() {
		return m_default.m_driverClassName;
	}

	/**
	 * @return Database schema name
	 */
	public String getSchemaName() {
		return m_default.m_schemaName;
	}

	/**
	 * @return Maximum intermediate buffer size. Should atleast be long enough
	 *         to hold the longest row. Impacts performance.
	 */
	public int maxBufferSize() {
		return m_default.m_bufferMaxSize;
	}

	/**
	 * @return Absolute target location for the dump file.
	 */
	public String getDumpFilePath() {
		return m_default.m_dumpFilePath;
	}
	
	class innerConfig{
		private String m_userName;
		private String m_password;
		private String m_driverClassName;
		private String m_derbyDbPath;
		private String m_schemaName;
		private int m_bufferMaxSize;
		private String m_dumpFilePath;
		
		private innerConfig() {
			Properties prop = PropertyLoader.loadProperties("dump");
			m_userName = prop.getProperty("db.userName");
			m_password = prop.getProperty("db.password");
			m_derbyDbPath = prop.getProperty("db.derbyDbPath");
			m_driverClassName = prop.getProperty("db.driverClassName");
			m_schemaName = prop.getProperty("db.schemaName");
			m_bufferMaxSize = Integer.valueOf(prop.getProperty("dump.buffer.size"));
			m_dumpFilePath = prop.getProperty("dump.buffer.dumpPath");
		}
		
		private innerConfig(String userName, String password, String derbyPath, String driverName, String schema, int maxBufferSize, String dumpLocation) {
			m_userName = userName;
			m_password = password;
			m_derbyDbPath = derbyPath;
			m_driverClassName = driverName;
			m_schemaName = schema;
			m_bufferMaxSize = Integer.valueOf(maxBufferSize);
			m_dumpFilePath = dumpLocation;
		}
	}
}
