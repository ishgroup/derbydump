package com.db.exporter.config;

import java.util.Properties;

/**
 * Singleton: Loads relevant settings from properties file.
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
		m_schemaName =  prop.getProperty("db.schemaName");
		m_bufferMaxSize = Integer.valueOf(prop.getProperty("dump.buffer.size"));
		m_dumpFilePath = prop.getProperty("dump.buffer.dumpPath");
	}

	public static synchronized Configuration getConfiguration() {
		if (m_this == null) {
			m_this = new Configuration();
		}
		return m_this;
	}

	public String getUserName() {
		return m_userName;
	}

	public String getPassword() {
		return m_password;
	}

	public String getDerbyDbPath() {
		return m_derbyDbPath;
	}

	public String getDriverName() {
		return m_driverClassName;
	}

	public String getSchemaName() {
		return m_schemaName;
	}
	
	public int maxBufferSize() {
		return m_bufferMaxSize;
	}

	public String getDumpFilePath() {
		return m_dumpFilePath;
	}
}
