package com.db.exporter.config;

import java.util.Properties;

/**
 * Singleton: Loads relevant application settings from properties file.
 * 
 */
public class Configuration {

	private static Configuration m_this;

	private String m_userName;
	private String m_password;
	private String m_driverClassName;
	private String m_derbyDbPath;
	private String m_schemaName;
	private int m_bufferMaxSize;
	private String m_dumpFilePath;

	private Configuration() {
		Properties prop = PropertyLoader.loadProperties("dump");
		m_userName = prop.getProperty("db.userName");
		m_password = prop.getProperty("db.password");
		m_derbyDbPath = prop.getProperty("db.derbyDbPath");
		m_driverClassName = prop.getProperty("db.driverClassName");
		m_schemaName = prop.getProperty("db.schemaName");
		m_bufferMaxSize = Integer.valueOf(prop.getProperty("dump.buffer.size"));
		m_dumpFilePath = prop.getProperty("dump.buffer.dumpPath");
	}

	public static synchronized Configuration getConfiguration() {
		if (m_this == null) {
			m_this = new Configuration();
		}
		return m_this;
	}

	/**
	 * @return Database userName
	 */
	public String getUserName() {
		return m_userName;
	}

	/**
	 * @return Database password
	 */
	public String getPassword() {
		return m_password;
	}

	/**
	 * @return Absolute path to the database
	 */
	public String getDerbyDbPath() {
		return m_derbyDbPath;
	}

	/**
	 * @return JDBC driver name
	 */
	public String getDriverName() {
		return m_driverClassName;
	}

	/**
	 * @return Database schema name
	 */
	public String getSchemaName() {
		return m_schemaName;
	}

	/**
	 * @return Maximum intermediate buffer size. Should atleast be long enough
	 *         to hold the longest row. Impacts performance.
	 */
	public int maxBufferSize() {
		return m_bufferMaxSize;
	}

	/**
	 * @return Absolute target location for the dump file.
	 */
	public String getDumpFilePath() {
		return m_dumpFilePath;
	}
}
