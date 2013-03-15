package com.db.exporter.reader.impl;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.List;

import org.apache.log4j.Logger;

import com.db.exporter.beans.Column;
import com.db.exporter.beans.Database;
import com.db.exporter.beans.Table;
import com.db.exporter.reader.IDatabaseReader;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.HexUtils;
import com.db.exporter.utils.StringUtils;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.IBuffer;

/**
 * This class implements {@link IDatabaseReader} and encapsulates methods for
 * reading data present inside the derby database.
 * 
 * @author Abhijeet
 * 
 */
public class DatabaseReader implements IDatabaseReader,Runnable {

	private IBuffer m_buffer;
	public final static String SEPARATOR = ",";
	private static final Logger LOGGER = Logger.getLogger(DatabaseReader.class);

	public DatabaseReader() {
		m_buffer = BufferManager.getBufferInstance();
	}
	
	/**
	 * This method reads data from the derby database
	 */
	public void readMetaData() {
		// getting the connection
		Connection connection;
		try {
			connection = DBConnectionManager.getConnection();
		} catch (SQLException e1) {
			LOGGER.error("Could not establish Database connection.", e1);
			return;
		}
		// creating a skeleton of tables and columns present in the database
		MetadataReader metadata = new MetadataReader();
		Database database = metadata.readDatabase(connection);
		getInternalData(database.getTables(), connection);
		if(connection != null){
		    try{
		        connection.close();
		    }
		    catch(SQLException e){
		    	LOGGER.error(e.getMessage());	
		    }
		}
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
		Statement statement = null;
		ResultSet resultSet = null;
		// Iterating over the list of the tables
		for (int t_index = 0; t_index < numOfTables; t_index++) {
			boolean t_flag = false;
			// picking a table at index
			Table table = tables.get(t_index);
			// fetching the atributes of the table
			String tableName = table.getTableName();
			List<Column> columns = table.getColumns();
			int numOfColumns = columns.size();
			//int numOfRows = table.getNumOfRows();
			// constructing the select query
			String selectQuery = StringUtils.getSelectQuery(tableName);
			String countQuery = StringUtils.getCountQuery(tableName);
			try {
				statement = connection.createStatement();
				resultSet = statement.executeQuery(countQuery);
				resultSet.next();
				int numOfRows = resultSet.getInt(1);
				resultSet = statement.executeQuery(selectQuery);
				int counter = 0;
				while (resultSet.next()) {
				    if(counter == 0){
				        m_buffer.add("LOCK TABLES `" + tableName
	                            + "` WRITE;" + "\n");
	                    m_buffer.add("INSERT INTO " + tableName + " VALUES ");
				    }
				    //TODO Logic needs to be refined for end of table data.
					if (counter == numOfRows - 1)
						t_flag = true;
					m_buffer.add("(");
					for (int c_index = 0; c_index < numOfColumns; c_index++) {
						boolean flag = false;
						if (c_index > 0){
						    flag = true;
						    m_buffer.add(SEPARATOR);
						}
						Column column = columns.get(c_index);
						String columnName = column.getColumnName();
						int columnType = column.getColumnDataType();
						switch(columnType){
                        case Types.BINARY :
                        case Types.VARBINARY :
                        case Types.BLOB :
                        {
                            byte[] bytes = resultSet.getBytes(columnName);
                            m_buffer.add(processBinaryData(bytes));
                            break;
                        }
                        case Types.CLOB :
                        {
                            Clob clob = resultSet.getClob(columnName);
                            m_buffer.add(processClobData(clob));
                            break;
                        }
                        case Types.CHAR :
                        case Types.LONGNVARCHAR :
                        case Types.VARCHAR :
                        {
                            String stringData = resultSet.getString(columnName);
                            m_buffer.add(processStringData(stringData));
                            break;
                        }
                        case Types.TIME :
                            String timeData = resultSet.getTime(
                                    columnName).toString();
                            m_buffer.add(processStringData(timeData));
                            break;
                        case Types.DATE :
                        {
                            String dateData = resultSet.getDate(
                                    columnName).toString();
                            m_buffer.add(processStringData(dateData));
                            break;
                        }
                        case Types.TIMESTAMP :
                        {
                            String stringData = resultSet.getTimestamp(
                                    columnName).toString();
                            m_buffer.add(processStringData(stringData));
                            break;
                        }
                        case Types.SMALLINT :
                            Short shortData = resultSet.getShort(columnName);
                            m_buffer.add(String.valueOf(shortData));
                            break;
                        case Types.BIGINT :
                            Long longData = resultSet.getLong(columnName);
                            m_buffer.add(String.valueOf(longData));
                            break;
                        case Types.INTEGER :
                        {
                            int data = resultSet.getInt(columnName);
                            m_buffer.add(String.valueOf(data));
                            break;
                        }
                        case Types.NUMERIC :
                        case Types.DECIMAL :
                            BigDecimal decimalData = resultSet.getBigDecimal(columnName);
                            m_buffer.add(String.valueOf(decimalData));
                            break;
                        case Types.REAL :
                        case Types.FLOAT :
                            Float floatData = resultSet.getFloat(columnName);
                            m_buffer.add(String.valueOf(floatData));
                            break;
                        case Types.DOUBLE :
                            Double doubleData = resultSet.getDouble(columnName);
                            m_buffer.add(String.valueOf(doubleData));
                            break;
                        default :
                        {
                            Object object = resultSet.getObject(columnName);
                            m_buffer.add(object.toString());
                        }
                    }
					}
					counter++;
					if (!t_flag){
						m_buffer.add("),\n");
					}else{
						m_buffer.add(");\n").add(StringUtils
								.getUnLockStatement(tableName));
					}
					
					if(m_buffer.isFull()){
						synchronized (BufferManager.BUFFER_TOKEN) {
							BufferManager.BUFFER_TOKEN.notify();
							BufferManager.BUFFER_TOKEN.wait();
						}
					}
				}
			} catch (SQLException e) {
				LOGGER.error(e.getMessage(), e);
				System.out.println(e.getCause());
			} catch (InterruptedException e) {
				LOGGER.error(DatabaseReader.class.getName()+ ": Interrupted. Exiting.");
				return;
			}finally{
				if(resultSet!=null)
					try {
						resultSet.close();
					} catch (SQLException e1) {
						LOGGER.error(e1.getMessage(), e1);
					}
				if(statement!=null)
					try {
						statement.close();
					} catch (SQLException e) {
						LOGGER.error(e.getMessage(), e);
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
	 * handles the binary data coming from the database
	 * 
	 * @param binaryData
	 * @param flag
	 * @return
	 */
	public String processBinaryData(byte[] binaryData) {
		String data = HexUtils.bytesToString(binaryData);
		data = "'0x" + data + "'";
		return data;
	}

	/**
	 * handles the {@link Clob} data coming from the database
	 * 
	 * @param data
	 * @param flag
	 * @return
	 */
	public String processClobData(Clob data) {
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
		result = "'" + result + "'";
		return result;
	}

	/**
	 * handles the string data coming from the database
	 * 
	 * @param data
	 * @param flag
	 * @return
	 */
	private String processStringData(String data) {
	    data = StringUtils.escapeQuotes(data);
		data = "'" + data + "'";
		return data;
	}

    public void run() {
        this.readMetaData();
    }
}
