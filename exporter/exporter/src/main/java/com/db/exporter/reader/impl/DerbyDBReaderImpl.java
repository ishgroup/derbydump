package com.db.exporter.reader.impl;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.db.exporter.Constants;
import com.db.exporter.beans.Column;
import com.db.exporter.beans.Table;
import com.db.exporter.config.Configuration;
import com.db.exporter.reader.DerbyDBReader;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.HexUtils;
import com.db.exporter.utils.IOUtils;
import com.db.exporter.utils.StringUtils;
import com.db.exporter.writer.QueueManager;

/**
 * This class implements {@link DerbyDBReader} and encapsulates methods for
 * reading data present inside the derby database.
 * 
 * @author Abhijeet
 * 
 */
public class DerbyDBReaderImpl implements DerbyDBReader {

	private DBConnectionManager dbConnectionManager;
	private QueueManager queueManager;
	private Configuration configuration;
	public final static String SEPARATOR = ",";
	static Log logger = LogFactory.getLog(DerbyDBReaderImpl.class);

	/**
	 * This method reads data from the derby database
	 */
	public void readMetaData() {
		// getting the connection
		Connection connection = dbConnectionManager.getConnection();
		// creating a skeleton of tables and columns present in the database
		List<Table> tables = getTables(connection);
		// fetching data from database and putting into the queue
		getInternalData(tables, connection);
	}

	/**
	 * This method is responsible for creating a skeleton of database present.
	 * Basically, {@link Table} and {@link Column} will be created with all
	 * attributes.
	 * 
	 * @param connection
	 * @return
	 */
	public List<Table> getTables(Connection connection) {
		List<Table> tables = new ArrayList<Table>();
		List<String> tableNames = getTableNames(connection);
		Iterator<String> iterator = tableNames.iterator();
		while (iterator.hasNext()) {
			String tableName = iterator.next();
			Table table = new Table();
			table.setTableName(tableName);
			List<Column> columns = new ArrayList<Column>();
			String selectQuery = StringUtils.getSelectQuery(tableName);
			String countQuery = StringUtils.getCountQuery(tableName);
			try {
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(countQuery);
                resultSet.next();
                int rowCount = resultSet.getInt(1);
				resultSet = statement.executeQuery(selectQuery);
				ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
				table.setNumOfRows(rowCount);
				int numOfCol = resultSetMetaData.getColumnCount();
				table.setNumOfColumns(numOfCol);
				for (int index = 1; index <= numOfCol; index++) {
					Column column = new Column();
					String columnName = resultSetMetaData.getColumnName(index);
					String columnDataType = resultSetMetaData
							.getColumnTypeName(index);
					String className = resultSetMetaData
							.getColumnClassName(index);
					column.setJavaClassName(className);
					column.setColumnName(columnName);
					column.setColumnDataType(columnDataType);
					columns.add(column);
				}
				table.setColumns(columns);
				resultSet.close();
				statement.close();
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
			tables.add(table);
		}
		return tables;
	}

	/**
	 * This method is responsible for fetching names of tables present in the
	 * database
	 * 
	 * @param connection
	 * @return
	 */
	private List<String> getTableNames(Connection connection) {
		List<String> tableNames = new ArrayList<String>();
		ResultSet resultSet = null;
		try {
			DatabaseMetaData databaseMetaData = connection.getMetaData();
			resultSet = databaseMetaData.getTables(null, null, null,
					new String[] { "TABLE" });
			while (resultSet.next()) {
				tableNames.add(resultSet.getString(3));
			}
		} catch (SQLException e) {
			logger.error(e.getMessage(), e);
		}
		finally{
		    try{
		        if(resultSet != null){
	                resultSet.close();
	            }
		    }
		    catch(Exception e){
		        e.printStackTrace();
		    }
		}
		return tableNames;
	}

	/**
	 * This method will read data from every {@link Table} persent in the
	 * database and add the data to the queue.
	 * 
	 * @param tables
	 * @param connection
	 */
	private void getInternalData(List<Table> tables, Connection connection) {
		int numOfTables = tables.size();
		// Iterating over the list of the tables
		for (int t_index = 0; t_index < numOfTables; t_index++) {
			boolean t_flag = false;
			// picking a table at index
			Table table = tables.get(t_index);
			// fetching the atributes of the table
			String tableName = table.getTableName();
			List<Column> columns = table.getColumns();
			int numOfColumns = table.getNumOfColumns();
			int numOfRows = table.getNumOfRows();
			// constructing the select query
			String selectQuery = StringUtils.getSelectQuery(tableName);
			try {
				Statement statement = connection.createStatement();
				ResultSet resultSet = statement.executeQuery(selectQuery);
				if (table.getNumOfRows() > 0) {
					String lockStatment = "LOCK TABLES `" + tableName
							+ "` WRITE;" + "\n";
					IOUtils.writeToFile(
							new File(configuration.getDumpFilePath()),
							lockStatment);
					String data = "INSERT INTO " + tableName + " VALUES ";
					IOUtils.writeToFile(
							new File(configuration.getDumpFilePath()), data);
				}
				int counter = 0;
				while (resultSet.next()) {
					if (counter == numOfRows - 1)
						t_flag = true;
					String subData = "(";
					for (int c_index = 0; c_index < numOfColumns; c_index++) {
						boolean flag = false;
						if (c_index > 0)
							flag = true;
						Column column = columns.get(c_index);
						String columnName = column.getColumnName();
						String columnType = column.getColumnDataType();

						if (columnType
								.equalsIgnoreCase(Constants.BINARY_DATA_TYPE)
								|| columnType
										.equalsIgnoreCase(Constants.BLOB_DATA_TYPE)
								|| columnType
										.equalsIgnoreCase(Constants.VARBINARY_DATA_TYPE)) {
							byte[] bytes = resultSet.getBytes(columnName);
							subData += processBinaryData(bytes, flag);
						} else if (columnType
								.equalsIgnoreCase(Constants.CLOB_DATA_TYPE)) {
							Clob clob = resultSet.getClob(columnName);
							subData += processClobData(clob, flag);
						} else if (columnType
								.equalsIgnoreCase(Constants.VARCHAR_DATA_TYPE)) {
							String stringData = resultSet.getString(columnName);
							subData += processStringData(stringData, flag);
						} else if (columnType
								.equalsIgnoreCase(Constants.TIMESTAMP_DATA_TYPE)) {
							String stringData = resultSet.getTimestamp(
									columnName).toString();
							subData += processStringData(stringData, flag);
						} else {
							Object object = resultSet.getObject(columnName);
							subData += object.toString();
						}
					}
					counter++;

					if (!t_flag)
						queueManager.addDataToQueue(subData + "),\n");
					else {
						queueManager.addDataToQueue(subData + ");\n");
						queueManager.addDataToQueue(StringUtils
								.getUnLockStatement(tableName));
					}
				}
				resultSet.close();
				statement.close();
			} catch (SQLException e) {
				logger.error(e.getMessage(), e);
			}
		}
		// reading from derby database is complete.
		queueManager.setReadingComplete(true);
	}

	/**
	 * @return the dbConnectionManager
	 */
	public DBConnectionManager getDbConnectionManager() {
		return dbConnectionManager;
	}

	/**
	 * @param dbConnectionManager
	 *            the dbConnectionManager to set
	 */
	public void setDbConnectionManager(DBConnectionManager dbConnectionManager) {
		this.dbConnectionManager = dbConnectionManager;
	}

	/**
	 * @return the queueManager
	 */
	public QueueManager getQueueManager() {
		return queueManager;
	}

	/**
	 * @param queueManager
	 *            the queueManager to set
	 */
	public void setQueueManager(QueueManager queueManager) {
		this.queueManager = queueManager;
	}

	/**
	 * handles the binary data coming from the database
	 * 
	 * @param binaryData
	 * @param flag
	 * @return
	 */
	public String processBinaryData(byte[] binaryData, boolean flag) {
		String data = HexUtils.bytesToString(binaryData);
		if (flag)
			data = SEPARATOR + " '" + data + "' ";
		return data;
	}

	/**
	 * handles the {@link Clob} data coming from the database
	 * 
	 * @param data
	 * @param flag
	 * @return
	 */
	public String processClobData(Clob data, boolean flag) {
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
			logger.error(e.getMessage(), e);
		} catch (IOException e) {
			logger.error(e.getMessage(), e);
		}
		String result = sb.toString();
		if (flag)
			result = SEPARATOR + " '" + result + "'";
		return result;
	}

	/**
	 * handles the string data coming from the database
	 * 
	 * @param data
	 * @param flag
	 * @return
	 */
	private String processStringData(String data, boolean flag) {
		if (flag)
			data = SEPARATOR + " '" + data + "'";
		else
			data = "'" + data + "'";
		return data;
	}

	/**
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * @param configuration
	 *            the configuration to set
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
