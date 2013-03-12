package com.db.exporter.utils;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This class is a utility class which contains method for creating strings with
 * different type of data which is useful in the application.
 * 
 * @author Abhijeet
 * 
 */
public class StringUtils {

	static Log logger = LogFactory.getLog(StringUtils.class);

	/**
	 * This method returns url used for connecting to the derby database.
	 * 
	 * @param derbyDbPath
	 * @param userName
	 * @param password
	 * @return
	 */
	public static String getDerbyUrl(String derbyDbPath, String userName,
			String password) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("jdbc:derby:");
		stringBuilder.append(derbyDbPath);
		stringBuilder.append(";create=true;");
		stringBuilder.append("user=" + userName + ";");
		stringBuilder.append("password=" + password + ";");
		String derbyDBUrl = stringBuilder.toString();
		return derbyDBUrl;
	}

	/**
	 * This method returns a string containing select query for table
	 * 
	 * @param tableName
	 * @return
	 */
	public static String getSelectQuery(String tableName) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT * FROM APP.");
		stringBuilder.append(tableName);
		return stringBuilder.toString();
	}

	/**
	 * This method returns a string containing count query for table which
	 * calculates the number of rows present in the table
	 * 
	 * @param tableName
	 * @return
	 */
	public static String getCountQuery(String tableName) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT COUNT(*) FROM APP.");
		stringBuilder.append(tableName);
		return stringBuilder.toString();
	}

	/**
	 * This method returns a string containing Unlocking statement for table
	 * 
	 * @param tableName
	 * @return
	 */
	public static String getUnLockStatement(String tableName) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("UNLOCK TABLES;");
		return stringBuilder.toString();
	}

}
