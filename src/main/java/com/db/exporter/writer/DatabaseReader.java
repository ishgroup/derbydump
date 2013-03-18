package com.db.exporter.writer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;

import com.db.exporter.beans.Column;
import com.db.exporter.beans.Database;
import com.db.exporter.beans.Table;
import com.db.exporter.config.Configuration;
import com.db.exporter.reader.IDatabaseReader;
import com.db.exporter.reader.impl.MetadataReader;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.HexUtils;
import com.db.exporter.utils.StringUtils;

/**
 * Logical module representing a reader/producer which reads from a database and
 * writes to a buffer.
 */
public class DatabaseReader implements IDatabaseReader, Runnable {

	private IBuffer m_buffer;
	private Configuration m_config;
	public final static String SEPARATOR = ",";
	private final static int MAX_ALLOWED_ROWS = 100; 
	private static final Logger LOGGER = Logger.getLogger(DatabaseReader.class);

	public DatabaseReader(Configuration config, IBuffer buffer) {
		m_buffer = buffer;
		m_config = config;
	}

	public void readMetaData(String schema) {
		// getting the connection
		Connection connection;
		try {
			connection = DBConnectionManager.getConnection(StringUtils
					.getDerbyUrl(m_config.getDerbyDbPath(),
							m_config.getUserName(), m_config.getPassword()));
		} catch (SQLException e1) {
			LOGGER.error("Could not establish Database connection.", e1);
			return;
		}
		// creating a skeleton of tables and columns present in the database
		MetadataReader metadata = new MetadataReader();
		Database database = metadata.readDatabase(connection);
		getInternalData(database.getTables(), connection, schema);
		if (connection != null) {
			try {
				connection.close();
			} catch (SQLException e) {
				LOGGER.error(e.getMessage());
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
		int numOfTables = tables.size();
		Statement statement = null;
		ResultSet resultSet = null;
		// Iterating over the list of the tables
		for (int t_index = 0; t_index < numOfTables; t_index++) {
		    //Flag for reading end of table
			boolean t_flag = false;
			StringBuilder initTableInsert = new StringBuilder();
			// picking a table at index
			Table table = tables.get(t_index);
			// fetching the attributes of the table
			String tableName = table.getTableName();
			List<Column> columns = table.getColumns();
			int numOfColumns = columns.size();
			// constructing the select query
			String selectQuery = StringUtils.getSelectQuery(tableName, schema);
			String countQuery = StringUtils.getCountQuery(tableName, schema);
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
						    if(c_index == 0){
						        initTableInsert.append("(");
						    }
						    initTableInsert.append(columns.get(c_index).getColumnName());
						    if(c_index == numOfColumns -1){
						        initTableInsert.append(") VALUES \n");
						    }
						    else{
						        initTableInsert.append(", ");
						    }
						}
						m_buffer.add(initTableInsert.toString());
					}
					// TODO Logic needs to be refined for end of table data.
					if (counter == numOfRows - 1)
						t_flag = true;
					m_buffer.add("(");
					for (int c_index = 0; c_index < numOfColumns; c_index++) {
						if (c_index > 0) {
							m_buffer.add(SEPARATOR);
						}
						Column column = columns.get(c_index);
						String columnName = column.getColumnName();
						int columnType = column.getColumnDataType();
						switch (columnType) {
						case Types.BINARY:
						case Types.VARBINARY:
						case Types.BLOB: {
							byte[] bytes = resultSet.getBytes(columnName);
							m_buffer.add(bytes == null ? null : processBinaryData(bytes));
							break;
						}
						case Types.CLOB: {
							Clob clob = resultSet.getClob(columnName);
							m_buffer.add(clob == null ? null : processClobData(clob));
							break;
						}
						case Types.CHAR:
						case Types.LONGNVARCHAR:
						case Types.VARCHAR: {
							String stringData = resultSet.getString(columnName);
							m_buffer.add(processStringData(stringData));
							break;
						}
						case Types.TIME: {
							Time obj = resultSet.getTime(columnName);
							String timeData = obj == null ? null : obj
									.toString();
							m_buffer.add(processStringData(timeData));
							break;
						}
						case Types.DATE: {
							Date obj = resultSet.getDate(columnName);
							String dateData = obj == null ? null : obj
									.toString();
							m_buffer.add(processStringData(dateData));
							break;
						}
						case Types.TIMESTAMP: {
							Timestamp obj = resultSet.getTimestamp(columnName);
							String stringData = obj == null ? null : obj
									.toString();
							m_buffer.add(processStringData(stringData));
							break;
						}
						case Types.SMALLINT:
							Short shortData = resultSet.getShort(columnName);
							m_buffer.add(shortData == null ? null : String
									.valueOf(shortData));
							break;
						case Types.BIGINT:
							Long longData = resultSet.getLong(columnName);
							m_buffer.add(longData == null ? null : String
									.valueOf(longData));
							break;
						case Types.INTEGER: {
							int data = resultSet.getInt(columnName);
							m_buffer.add(String.valueOf(data));
							break;
						}
						case Types.NUMERIC:
						case Types.DECIMAL:
							BigDecimal decimalData = resultSet
									.getBigDecimal(columnName);
							m_buffer.add(decimalData == null ? null : String
									.valueOf(decimalData));
							break;
						case Types.REAL:
						case Types.FLOAT:
							Float floatData = resultSet.getFloat(columnName);
							m_buffer.add(floatData == null ? null : String
									.valueOf(floatData));
							break;
						case Types.DOUBLE:
							Double doubleData = resultSet.getDouble(columnName);
							m_buffer.add(doubleData == null ? null : String
									.valueOf(doubleData));
							break;
						default: {
							Object object = resultSet.getObject(columnName);
							m_buffer.add(object == null ? null : object
									.toString());
						}
						}
					}
					counter++;
					if (!t_flag && counter%MAX_ALLOWED_ROWS != 0){
						m_buffer.add("),\n");
					}
					else if(!t_flag){
					    m_buffer.add(");\n").add(
                                StringUtils.getUnLockStatement(tableName));
					    m_buffer.add(initTableInsert.toString());
					}
					else {
						m_buffer.add(");\n").add(
								StringUtils.getUnLockStatement(tableName)).add("\n");
					}

					if (m_buffer.isFull()) {
						synchronized (BufferManager.BUFFER_TOKEN) {
							BufferManager.BUFFER_TOKEN.notify();
							BufferManager.BUFFER_TOKEN.wait();
						}
					}
				}
			} catch (SQLException e) {
				LOGGER.error(e.getMessage(), e);
			} catch (InterruptedException e) {
				LOGGER.error(DatabaseReader.class.getName()
						+ ": Database Reader interrupted - exiting.");
				return;
			} finally {
				if (resultSet != null)
					try {
						resultSet.close();
					} catch (SQLException e1) {
						LOGGER.error(e1.getErrorCode()
								+ ": Could not close the resultset: "
								+ e1.getMessage());
					}
				if (statement != null)
					try {
						statement.close();
					} catch (SQLException e) {
						LOGGER.error(e.getErrorCode()
								+ ": Could not close the statement: "
								+ e.getMessage());
					}
			}
		}
		// reading from derby database is complete.
		BufferManager.setReadingComplete(true);
		synchronized (BufferManager.BUFFER_TOKEN) {
			BufferManager.BUFFER_TOKEN.notify();
		}
	}

	/**
	 * @param binaryData
	 * @return String representation of binary data
	 */
	public String processBinaryData(byte[] binaryData) {
		if (binaryData == null)
			return null;
		String data = HexUtils.bytesToString(binaryData);
		data = "'" + data + "'";
		return data;
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
			LOGGER.error(e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
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

		data = StringUtils.escapeQuotes(data.trim());
		data = "'" + data + "'";
		return data;
	}
	
	public void run() {
		this.readMetaData(m_config.getSchemaName());
	}
}