package com.db.exporter.utils;

import org.apache.log4j.Logger;

/**
 * Utility class: Provides database related string manipulation methods.
 */

public class StringUtils {

	static Logger LOGGER = Logger.getLogger(StringUtils.class);

	/**
	 * @param tableName
	 *            Database table name
	 * @param schema
	 *            Database schema name
	 * 
	 * @return Returns a string representation of the select query for table.
	 */
	public static String getSelectQuery(String tableName, String schema) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT * FROM " + schema + ".");
		stringBuilder.append(tableName);
		return stringBuilder.toString();
	}

	/**
	 * Computes a string representation of the count query for a table,
	 * calculating the number of rows present in the table.
	 * 
	 * @param tableName
	 * @param schema
	 *            Database schema name
	 * 
	 * @return Returns a string representation of the count query for a table
	 */
	public static String getCountQuery(String tableName, String schema) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT COUNT(*) FROM " + schema + ".");
		stringBuilder.append(tableName);
		return stringBuilder.toString();
	}

	/**
	 * @param tableName
	 * 
	 * @return Returns a string containing Unlocking statement for table
	 */
	public static String getUnLockStatement(String tableName) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("UNLOCK TABLES;");
		return stringBuilder.toString();
	}

	/**
	 * Escapes sql special characters
	 * 
	 * @param raw
	 * 
	 * @return Escaped query
	 */
	public static String escapeQuotes(String raw) {
		StringBuilder cooked = new StringBuilder();
		char c;
		for (int i = 0; i < raw.length(); i++) {
			c = raw.charAt(i);
			if (c == '\'') {
				cooked = cooked.append('\'').append(c);
			} else {
				cooked = cooked.append(c);
			}
		}
		return cooked.toString();
	}
}
