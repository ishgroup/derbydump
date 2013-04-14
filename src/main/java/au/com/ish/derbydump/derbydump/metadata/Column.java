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

package au.com.ish.derbydump.derbydump.metadata;

import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.*;

/**
 * Represents a column in a database table.
 *
 */
public class Column {

	private static final Logger LOGGER = Logger.getLogger(Column.class);
	/**
	 * Name of the column
	 */
	private String columnName;
	/**
	 * Data type of the column
	 */
	private int columnDataType;
	/**
	 * Constraint type of a column(if any).
	 */
	private String constraintType;
	/**
	 * Name of java class which represents the data type of the column
	 */
	private String javaClassName;
	
	/**
	 * @return the columnName
	 */
	public String getColumnName() {
		return columnName;
	}
	/**
	 * @param columnName the columnName to set
	 */
	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}
	/**
	 * @return the columnDataType
	 */
	public int getColumnDataType() {
		return columnDataType;
	}
	/**
	 * @param columnDataType the columnDataType to set
	 */
	public void setColumnDataType(int columnDataType) {
		this.columnDataType = columnDataType;
	}
	/**
	 * @return the constraintType
	 */
	public String getConstraintType() {
		return constraintType;
	}
	/**
	 * @param constraintType the constraintType to set
	 */
	public void setConstraintType(String constraintType) {
		this.constraintType = constraintType;
	}
	/**
	 * @return the javaClassName
	 */
	public String getJavaClassName() {
		return javaClassName;
	}
	/**
	 * @param javaClassName the javaClassName to set
	 */
	public void setJavaClassName(String javaClassName) {
		this.javaClassName = javaClassName;
	}

	/**
	 * Get a string value for the value in this column in the datarow
	 * 
	 * @param dataRow
	 * @return an SQL statement compliant string version of the value
	 */
	public String toString(ResultSet dataRow) throws SQLException {

		switch (getColumnDataType()) {
			case Types.BINARY:
			case Types.VARBINARY:
			case Types.BLOB: {
				byte[] obj = dataRow.getBytes(columnName);
				return (obj == null) ? "NULL" : processBinaryData(obj);
			}
			
			case Types.CLOB: {
				Clob obj = dataRow.getClob(columnName);
				return (obj == null) ? "NULL" : processClobData(obj);
			}
			
			case Types.CHAR:
			case Types.LONGNVARCHAR:
			case Types.VARCHAR: {
				String obj = dataRow.getString(columnName);
				return (obj == null) ? "NULL" : processStringData(obj);
			}
			
			case Types.TIME: {
				Time obj = dataRow.getTime(columnName);
				return (obj == null) ? "NULL" : processStringData(obj.toString());
			}
			
			case Types.DATE: {
				Date obj = dataRow.getDate(columnName);
				return (obj == null) ? "NULL" : processStringData(obj.toString());
			}
			
			case Types.TIMESTAMP: {
				Timestamp obj = dataRow.getTimestamp(columnName);
				return (obj == null) ? "NULL" : processStringData(obj.toString());
			}
			
			case Types.SMALLINT: {
				Object obj = dataRow.getObject(columnName);
				return (obj == null) ? "NULL" : obj.toString();
			}
			
			case Types.BIGINT: {
				Object obj = dataRow.getObject(columnName);
				return (obj == null) ? "NULL" : obj.toString();
			}
			
			case Types.INTEGER: {
				Object obj = dataRow.getObject(columnName);
				return (obj == null) ? "NULL" : obj.toString();
			}
			
			case Types.NUMERIC:
			case Types.DECIMAL: {
				BigDecimal obj = dataRow.getBigDecimal(columnName);
				return (obj == null) ? "NULL" : String.valueOf(obj);
			}
			
			case Types.REAL:
			case Types.FLOAT: {
				Float obj = dataRow.getFloat(columnName);
				return (obj == null) ? "NULL" : String.valueOf(obj);
			}
			
			case Types.DOUBLE: {
				Double obj = dataRow.getDouble(columnName);
				return (obj == null) ? "NULL" : String.valueOf(obj);
			}
			
			default: {
				Object obj = dataRow.getObject(columnName);
				return (obj == null) ? "NULL" : obj.toString();
			}
		}
	}


	/**
	 * @param binaryData
	 * @return String representation of binary data
	 */
	String processBinaryData(byte[] binaryData) {
		if (binaryData == null) {
			return "NULL";
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
			return "NULL";

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
			return "NULL";

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
		String output;
		
		// Replace "\" with "\\"
		output = raw.replaceAll("\\\\", "\\\\\\\\");
		
		// Replace ASCII NUL
		output = output.replaceAll("\\x00", "\\\\0");

		// Replace tab with "\t"
		output = output.replaceAll("\\x09", "\\\\t");

		// Replace backspace with "\b"
		output = output.replaceAll("\\x08", "\\\\b");

		// Replace newline with "\n"
		output = output.replaceAll("\\n", "\\\\n");

		// Replace carriage return with "\r"
		output = output.replaceAll("\\r", "\\\\r");

		// Replace ASCII 26 (Windows eof)
		output = output.replaceAll("\\x1a", "\\\\Z");
		
		// Replace "'" with "\'"
		output = output.replaceAll("\'", "\\\\'");
		
		return output;
	}
}
