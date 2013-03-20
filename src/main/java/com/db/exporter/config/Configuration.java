package com.db.exporter.config;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Loads relevant application settings from properties file, by default.
 * 
 */
public class Configuration {

	private static Configuration configuration;
	private Properties prop = new Properties();

	private Configuration() {
		try {
			FileInputStream file = new FileInputStream("derbydump.properties");
			prop.load(file);
			file.close();
		} catch (Exception ignored) {}

	}

	public static synchronized Configuration getConfiguration() {
		if (configuration == null) {
			configuration = new Configuration();
		}
		return configuration;
	}


	public String getUserName() {
		return prop.getProperty("db.userName");
	}

	public void setUserName(String userName) {
		prop.setProperty("db.userName", userName);
	}

	public String getPassword() {
		return prop.getProperty("db.password");
	}

	public void setPassword(String password) {
		prop.setProperty("db.password", password);
	}

	public String getDriverClassName() {
		return prop.getProperty("db.driverClassName");
	}

	public void setDriverClassName(String driverClassName) {
		prop.setProperty("db.driverClassName", driverClassName);
	}

	public String getDerbyDbPath() {
		return prop.getProperty("db.derbyDbPath");
	}

	public void setDerbyDbPath(String derbyDbPath) {
		prop.setProperty("db.derbyDbPath", derbyDbPath);
	}

	public String getSchemaName() {
		return prop.getProperty("db.schemaName");
	}

	public void setSchemaName(String schemaName) {
		prop.setProperty("db.schemaName", schemaName);
	}

	public int getBufferMaxSize() {
		if (prop.getProperty("dump.buffer.size") == null) {
			return 8192;
		}
		return  Integer.valueOf(prop.getProperty("dump.buffer.size"));
	}

	public void setBufferMaxSize(int bufferMaxSize) {
		prop.setProperty("dump.buffer.size", "" + bufferMaxSize);
	}

	public String getOutputFilePath() {
		return prop.getProperty("outputPath");
	}

	public void setOutputFilePath(String outputFilePath) {
		prop.setProperty("outputPath", outputFilePath);
	}
}
