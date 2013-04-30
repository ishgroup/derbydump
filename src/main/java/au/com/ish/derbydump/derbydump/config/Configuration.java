/*
 * Copyright 2013 ish group pty ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.com.ish.derbydump.derbydump.config;

import java.io.FileInputStream;
import java.util.Properties;

/**
 * Loads relevant application settings from properties file, by default.
 * 
 */
public class Configuration {

	private static Configuration configuration;
	private Properties prop = new Properties();
	private Properties tableRewriteProp = new Properties();

	private Configuration() {
		try {
			FileInputStream file = new FileInputStream("derbydump.properties");
			prop.load(file);
			file.close();

			if (getTableRewritePath() != null && getTableRewritePath().length() > 0) {
				file = new FileInputStream(getTableRewritePath());
				tableRewriteProp.load(file);
				file.close();
				for (String entry : tableRewriteProp.stringPropertyNames()) {
					// put a copy of every entry into the properties as lowercase for case-insensitive matching later
					tableRewriteProp.setProperty(entry.toLowerCase(), tableRewriteProp.getProperty(entry));
				}
			}

		} catch (Exception ignored) {}

	}

	public static synchronized Configuration getConfiguration() {
		if (configuration == null) {
			configuration = new Configuration();
		}
		return configuration;
	}

	public String getDerbyUrl() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("jdbc:derby:");
		stringBuilder.append(getDerbyDbPath());
		stringBuilder.append(";create=true;");
		stringBuilder.append("user=").append(getUserName()).append(";");
		stringBuilder.append("password=").append(getPassword()).append(";");
		stringBuilder.append("create=false;");

		return stringBuilder.toString();
	}


	public void setTableRewriteProperty(String key, String value) {
		tableRewriteProp.setProperty(key.toLowerCase(), value);
	}


	public String rewriteTableName(String tableName) {
		String newName = tableRewriteProp.getProperty(tableName.toLowerCase());
		if (newName != null) {
			return newName.trim();
		}
		return tableName;
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
		return  Integer.valueOf(prop.getProperty("dump.buffer.size").trim());
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

	public String getTableRewritePath() {
		return prop.getProperty("tableRewritePath");
	}

	public void setTableRewritePath(String filePath) {
		prop.setProperty("tableRewritePath", filePath);
	}
}
