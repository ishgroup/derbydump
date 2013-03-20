package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.metadata.Column;
import com.db.exporter.metadata.Database;
import com.db.exporter.metadata.Table;
import com.db.exporter.utils.DBConnectionManager;
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
public class DatabaseReader implements Runnable {

	public final static String SEPARATOR = ",";
	private final static int MAX_ALLOWED_ROWS = 100; 
	private static final Logger LOGGER = Logger.getLogger(DatabaseReader.class);
	private final OutputThread output;

	private Configuration config;

	public DatabaseReader(OutputThread output) {
		this.output = output;
		config = Configuration.getConfiguration();
	}

	public void readMetaData(String schema) {
		// getting the connection
		Connection connection;
		try {
			connection = DBConnectionManager.getConnection(config.getDerbyUrl());
		} catch (SQLException e1) {
			LOGGER.error("Could not establish Database connection.", e1);
			return;
		}
		// creating a skeleton of tables and columns present in the database
		MetadataReader metadata = new MetadataReader();
		LOGGER.debug("Resolving database structure...");
		Database database = metadata.readDatabase(connection);
		getInternalData(database.getTables(), connection, schema);
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOGGER.error("Could not close database connection :" + e.getErrorCode() + " - " + e.getMessage());
			}
		}
	}

	/**
	 * Read data from every {@link Table} present in the database and add it to
	 * the queue.
	 * 
	 * @param tables
	 * @param connection
	 */
	private void getInternalData(List<Table> tables, Connection connection,
			String schema) {
		LOGGER.debug("Fetching database data...");

		Statement statement = null;
		ResultSet resultSet = null;
		
		output.add("SET FOREIGN_KEY_CHECKS = 0;");
		// Iterating over the list of the tables
		for (Table table : tables) {

			//Flag for reading end of table
			boolean t_flag = false;
			StringBuilder initTableInsert = new StringBuilder();
			// picking a table at index

			// fetching the attributes of the table
			String tableName = table.getTableName();
			List<Column> columns = table.getColumns();
			int numOfColumns = columns.size();
			// constructing the select query
			String selectQuery = table.getSelectQuery(schema);
			String countQuery = table.getCountQuery(schema);
			try {
				statement = connection.createStatement();
				resultSet = statement.executeQuery(countQuery);
				resultSet.next();
				int numOfRows = resultSet.getInt(1);
				resultSet = statement.executeQuery(selectQuery);
				int counter = 0;
				while (resultSet.next()) {
					if (counter == 0) {
						initTableInsert.append("LOCK TABLES `" + tableName + "` WRITE;\n");
						initTableInsert.append("INSERT INTO " + tableName + " ");
						for (int c_index = 0; c_index < numOfColumns; c_index++) {
							//String columnName = columns.get(c_index).getColumnName();
							if (c_index == 0) {
								initTableInsert.append("(");
							}
							initTableInsert.append(columns.get(c_index).getColumnName());
							if (c_index == numOfColumns - 1) {
								initTableInsert.append(") VALUES \n");
							} else {
								initTableInsert.append(", ");
							}
						}
						output.add(initTableInsert.toString());
					}
					// TODO Logic needs to be refined for end of table data.
					if (counter == numOfRows - 1)
						t_flag = true;
					output.add("(");
					for (int c_index = 0; c_index < numOfColumns; c_index++) {
						if (c_index > 0) {
							output.add(SEPARATOR);
						}
						Column column = columns.get(c_index);
						String columnName = column.getColumnName();
						int columnType = column.getColumnDataType();
						switch (columnType) {
							case Types.BINARY:
							case Types.VARBINARY:
							case Types.BLOB: {
								byte[] bytes = resultSet.getBytes(columnName);
								output.add(bytes == null ? null : processBinaryData(bytes));
								break;
							}
							case Types.CLOB: {
								Clob clob = resultSet.getClob(columnName);
								output.add(clob == null ? null : processClobData(clob));
								break;
							}
							case Types.CHAR:
							case Types.LONGNVARCHAR:
							case Types.VARCHAR: {
								String stringData = resultSet.getString(columnName);
								output.add(processStringData(stringData));
								break;
							}
							case Types.TIME: {
								Time obj = resultSet.getTime(columnName);
								String timeData = obj == null ? null : obj
										.toString();
								output.add(processStringData(timeData));
								break;
							}
							case Types.DATE: {
								Date obj = resultSet.getDate(columnName);
								String dateData = obj == null ? null : obj
										.toString();
								output.add(processStringData(dateData));
								break;
							}
							case Types.TIMESTAMP: {
								Timestamp obj = resultSet.getTimestamp(columnName);
								String stringData = obj == null ? null : obj
										.toString();
								output.add(processStringData(stringData));
								break;
							}
							case Types.SMALLINT:
								Short shortData = resultSet.getShort(columnName);
								output.add(shortData == null ? null : String
										.valueOf(shortData));
								break;
							case Types.BIGINT:
								Long longData = resultSet.getLong(columnName);
								output.add(longData == null ? null : String
										.valueOf(longData));
								break;
							case Types.INTEGER: {
								int data = resultSet.getInt(columnName);
								output.add(String.valueOf(data));
								break;
							}
							case Types.NUMERIC:
							case Types.DECIMAL:
								BigDecimal decimalData = resultSet
										.getBigDecimal(columnName);
								output.add(decimalData == null ? null : String
										.valueOf(decimalData));
								break;
							case Types.REAL:
							case Types.FLOAT:
								Float floatData = resultSet.getFloat(columnName);
								output.add(floatData == null ? null : String
										.valueOf(floatData));
								break;
							case Types.DOUBLE:
								Double doubleData = resultSet.getDouble(columnName);
								output.add(doubleData == null ? null : String
										.valueOf(doubleData));
								break;
							default: {
								Object object = resultSet.getObject(columnName);
								output.add(object == null ? null : object
										.toString());
							}
						}
					}
					counter++;
					output.add(");\n");

					if (!t_flag && counter % MAX_ALLOWED_ROWS != 0) {

					} else if (!t_flag) {
						output.add(table.getUnLockStatement() + "\n");
						output.add(initTableInsert.toString());
					} else {
						output.add(table.getUnLockStatement() + "\n");
					}
				}
			} catch (SQLException e) {
				LOGGER.error("Error: " + e.getErrorCode() + " - " + e.getMessage());
			} finally {
				if (resultSet != null)
					try {
						resultSet.close();
					} catch (SQLException e1) {
						LOGGER.error("Could not close the resultset :" + e1.getErrorCode() + " - " + e1.getMessage());
					}
				if (statement != null)
					try {
						statement.close();
					} catch (SQLException e) {
						LOGGER.error("Could not close the statement :" + e.getErrorCode() + " - " + e.getMessage());
					}
			}
		}
		output.add("SET FOREIGN_KEY_CHECKS = 1;");
		LOGGER.debug("Reading done.");

		Thread.currentThread().interrupt();
	}

	/**
	 * @param binaryData
	 * @return String representation of binary data
	 */
	public String processBinaryData(byte[] binaryData) {
		if (binaryData == null) {
			return null;
		}

		Hex hexEncoder = new Hex(CharEncoding.UTF_8);
		return  "'" + new String(hexEncoder.encode(binaryData)) + "'";
	}

	/**
	 * @param data
	 * @return String representation of Clob.
	 */
	public String processClobData(Clob data) {
		if (data == null)
			return null;
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
		String result = sb.toString();
		result = processStringData(result);
		return result;
	}

	/**
	 * @param data
	 * @return String representation of string data after escaping.
	 */
	private String processStringData(String data) {
		if (data == null)
			return null;

		data = escapeQuotes(data.trim());
		data = "'" + data + "'";
		return data;
	}
	
	public void run() {
		LOGGER.debug("Database reader intializing...");
		this.readMetaData(config.getSchemaName());
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