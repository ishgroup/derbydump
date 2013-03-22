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

package au.com.ish.derbydump.derbydump.main;

import au.com.ish.derbydump.derbydump.config.Configuration;
import au.com.ish.derbydump.derbydump.metadata.Column;
import au.com.ish.derbydump.derbydump.metadata.Database;
import au.com.ish.derbydump.derbydump.metadata.Table;
import au.com.ish.derbydump.derbydump.config.DBConnectionManager;
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;
import java.util.List;

/**
 * Logical module representing a reader/producer which reads from a database and
 * writes to a buffer.
 */
public class DatabaseReader {

	private final static int MAX_ALLOWED_ROWS = 100;
	private static final Logger LOGGER = Logger.getLogger(DatabaseReader.class);
	private final OutputThread output;

	private Configuration config;

	public DatabaseReader(OutputThread output) {
		this.output = output;
		config = Configuration.getConfiguration();

		LOGGER.debug("Database reader initializing...");
		readMetaData(config.getSchemaName());
	}

	void readMetaData(String schema) {
		// getting the connection
		DBConnectionManager db;
		try {
			db = new DBConnectionManager(config.getDerbyUrl());
		} catch (Exception e) {
			LOGGER.error("Could not establish Database connection.", e);
			return;
		}
		// creating a skeleton of tables and columns present in the database
		MetadataReader metadata = new MetadataReader();
		LOGGER.debug("Resolving database structure...");
		Database database = metadata.readDatabase(db.getConnection());
		getInternalData(database.getTables(), db.getConnection(), schema);

		try {
			db.getConnection().close();
		} catch (SQLException e) {
			LOGGER.error("Could not close database connection :" + e.getErrorCode() + " - " + e.getMessage());
		}

	}

	/**
	 * Read data from every {@link Table} present in the database and add it to
	 * the output.
	 * 
	 * @param tables
	 * @param connection
	 */
	private void getInternalData(List<Table> tables, Connection connection, String schema) {
		LOGGER.debug("Fetching database data...");

		output.add("SET FOREIGN_KEY_CHECKS = 0;\n");

		for (Table table : tables) {
			if (!table.isExcluded()) {
				List<Column> columns = table.getColumns();
				LOGGER.info("Table " + table.getTableName() + "...\n");

				try {
					Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					ResultSet dataRows = statement.executeQuery(table.getSelectQuery(schema));
					int rowCount = 0;

					StringBuilder outputSQL = new StringBuilder();

					outputSQL.append("LOCK TABLES `" + table.getTableName() + "` WRITE;\n");
					outputSQL.append(table.getInsertSQL());

					while (dataRows.next()) {

						outputSQL.append("(");
						for (Column column : columns) {

							String columnName = column.getColumnName();
							int columnType = column.getColumnDataType();
							switch (columnType) {
								case Types.BINARY:
								case Types.VARBINARY:
								case Types.BLOB: {
									byte[] obj = dataRows.getBytes(columnName);
									outputSQL.append(obj == null ? "NULL" : processBinaryData(obj));
									break;
								}
								case Types.CLOB: {
									Clob obj = dataRows.getClob(columnName);
									outputSQL.append(obj == null ? "NULL" : processClobData(obj));
									break;
								}
								case Types.CHAR:
								case Types.LONGNVARCHAR:
								case Types.VARCHAR: {
									String obj = dataRows.getString(columnName);
									outputSQL.append(obj == null ? "NULL" : processStringData(obj));
									break;
								}
								case Types.TIME: {
									Time obj = dataRows.getTime(columnName);
									outputSQL.append(obj == null ? "NULL" : processStringData(obj.toString()));
									break;
								}
								case Types.DATE: {
									Date obj = dataRows.getDate(columnName);
									outputSQL.append(obj == null ? "NULL" : processStringData(obj.toString()));
									break;
								}
								case Types.TIMESTAMP: {
									Timestamp obj = dataRows.getTimestamp(columnName);
									outputSQL.append(obj == null ? "NULL" : processStringData(obj.toString()));
									break;
								}
								case Types.SMALLINT: {
									Object obj = dataRows.getObject(columnName);
									outputSQL.append(obj == null ? "NULL" : obj.toString());
									break;
								}
								case Types.BIGINT: {
									Long obj = dataRows.getLong(columnName);
									outputSQL.append(obj == null ? "NULL" : obj.toString());
									break;
								}
								case Types.INTEGER: {
									Object obj = dataRows.getObject(columnName);
									outputSQL.append(obj == null ? "NULL" : obj.toString());
									break;
								}
								case Types.NUMERIC:
								case Types.DECIMAL: {
									BigDecimal obj = dataRows.getBigDecimal(columnName);
									outputSQL.append(obj == null ? "NULL" : String.valueOf(obj));
									break;
								}
								case Types.REAL:
								case Types.FLOAT: {
									Float obj = dataRows.getFloat(columnName);
									outputSQL.append(obj == null ? "NULL" : String.valueOf(obj));
									break;
								}
								case Types.DOUBLE: {
									Double obj = dataRows.getDouble(columnName);
									outputSQL.append(obj == null ? "NULL" : String.valueOf(obj));
									break;
								}
								default: {
									Object obj = dataRows.getObject(columnName);
									outputSQL.append(obj == null ? "NULL" : obj.toString());
								}
							}
							outputSQL.append(",");
						}
						rowCount++;
						outputSQL.deleteCharAt(outputSQL.length()-1); //remove the last comma

						outputSQL.append(")");
						if (!dataRows.isLast()) {
							outputSQL.append(",");

							if (rowCount % MAX_ALLOWED_ROWS == 0) {
								outputSQL.deleteCharAt(outputSQL.length()-1); //remove the last comma
								outputSQL.append(";\n");
								outputSQL.append(table.getInsertSQL());
							}
						}
						outputSQL.append("\n");
					}

					outputSQL.deleteCharAt(outputSQL.length()-1); //remove the last comma
					outputSQL.append(";\n");

					outputSQL.append("UNLOCK TABLES;\n");

					output.add(outputSQL.toString());

					dataRows.close();
					statement.close();

				} catch (SQLException e) {
					LOGGER.error("Error: " + e.getErrorCode() + " - " + e.getMessage());
				}
			}
		}
		output.add("SET FOREIGN_KEY_CHECKS = 1;");
		LOGGER.debug("Reading done.");
	}

	/**
	 * @param binaryData
	 * @return String representation of binary data
	 */
	String processBinaryData(byte[] binaryData) {
		if (binaryData == null) {
			return "";
		}

		Hex hexEncoder = new Hex(CharEncoding.UTF_8);
		return  "'" + new String(hexEncoder.encode(binaryData)) + "'";
	}

	/**
	 * @param data
	 * @return String representation of Clob.
	 */
	String processClobData(Clob data) {
		if (data == null)
			return "";

		StringBuilder sb = new StringBuilder();
		try {
			Reader reader = data.getCharacterStream();
			BufferedReader br = new BufferedReader(reader);

			String line;
			while (null != (line = br.readLine())) {
				sb.append(line);
			}
			br.close();
		} catch (SQLException e) {
			LOGGER.error("Could not read data from stream :" + e.getErrorCode() + " - " + e.getMessage() + "\n"+ sb.toString());
		} catch (IOException e) {
			LOGGER.error("Could not read data from stream :" +  e.getMessage() + "\n"+ sb.toString());
		}
		return processStringData(sb.toString());
	}

	/**
	 * @param data
	 * @return String representation of string data after escaping.
	 */
	private String processStringData(String data) {
		if (data == null)
			return "";

		return "'" + escapeQuotes(data) + "'";
	}

	/**
	 * Escapes sql special characters
	 *
	 * @param raw
	 *
	 * @return Escaped query
	 */
	public static String escapeQuotes(String raw) {
		return raw.replaceAll("\'", "\'\'");
	}
}