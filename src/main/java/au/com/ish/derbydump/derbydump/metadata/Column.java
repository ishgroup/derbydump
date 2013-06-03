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

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.io.IOUtils;
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
				Blob obj = dataRow.getBlob(columnName);
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
				// dataRow.getFloat() always returns a value. only way to check the null is wasNull() method
				return (dataRow.wasNull()) ? "NULL" : String.valueOf(obj);
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
	 * this is a tricky one. according to
	 <ul>
	 <li>http://db.apache.org/derby/docs/10.2/ref/rrefjdbc96386.html</li>
	 <li>http://stackoverflow.com/questions/7510112/how-to-make-java-ignore-escape-sequences-in-a-string</li>
	 <li>http://dba.stackexchange.com/questions/10642/mysql-mysqldump-uses-n-instead-of-null</li>
	 <li>http://stackoverflow.com/questions/12038814/import-hex-binary-data-into-mysql</li>
	 <li>http://stackoverflow.com/questions/3126210/insert-hex-values-into-mysql</li>
	 <li>http://www.xaprb.com/blog/2009/02/12/5-ways-to-make-hexadecimal-identifiers-perform-better-on-mysql/</li>
	 </ul>
	 and many others, there is no safer way of exporting blobs than separate data files or hex format.<br/>
	 tested, mysql detects and imports hex encoded fields automatically.

	 * @param blob
	 * @return String representation of binary data
	 */
	public static String processBinaryData(Blob blob) throws SQLException {
		if (blob == null) {
			return "NULL";
		}
		int blobLength =  (int) blob.length();
		if (blobLength == 0) {
			return "NULL";
		}
		byte[] bytes = blob.getBytes(1L, blobLength);

		return "0x"+new String(Hex.encodeHex(bytes)).toUpperCase();
	}

	/**
	 * @param data
	 * @return String representation of Clob.
	 */
	public static String processClobData(Clob data) {
		if (data == null)
			return "NULL";

		Reader reader = null;
		BufferedReader br = null;
		try {
			reader = data.getCharacterStream();
			br = new BufferedReader(reader);

			return processStringData(IOUtils.toString(br));
		} catch (SQLException e) {
			LOGGER.error("Could not read data from stream :" + e.getErrorCode() + " - " + e.getMessage(), e);
		} catch (IOException e) {
			LOGGER.error("Could not read data from stream :" + e.getMessage(), e);
		} finally {
			IOUtils.closeQuietly(reader);
			IOUtils.closeQuietly(br);
		}
		return "NULL";
	}

	/**
	 * @param data
	 * @return String representation of string data after escaping.
	 */
	private static String processStringData(String data) {
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
