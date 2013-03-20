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

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Represents a database table.
 * 
 */
public class Table {

	/**
	 * List of Columns present in the table.
	 */
	private List<Column> columns = new ArrayList<Column>();
	/**
	 * Name of the table in the database.
	 */
	private String tableName;


	/**
	 * @param schema Database schema name
	 *
	 * @return Returns a string representation of the select query for table.
	 */
	public String getSelectQuery(String schema) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT * FROM ").append(schema).append(".");
		stringBuilder.append(tableName);
		return stringBuilder.toString();
	}

	/**
	 * Computes a string representation of the count query for a table,
	 * calculating the number of rows present in the table.
	 *
	 * @param schema Database schema name
	 *
	 * @return Returns a string representation of the count query for a table
	 */
	public String getCountQuery(String schema) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("SELECT COUNT(*) FROM ").append(schema).append(".");
		stringBuilder.append(tableName);
		return stringBuilder.toString();
	}

	/**
	 * @return Returns a string containing Unlocking statement for table
	 */
	public String getUnLockStatement() {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append("UNLOCK TABLES;");
		return stringBuilder.toString();
	}


	/**
	 * @return the columns
	 */
	public List<Column> getColumns() {
		return columns;
	}

	/**
	 * @return the tableName
	 */
	public String getTableName() {
		return tableName;
	}

	/**
	 * @param tableName
	 *            the tableName to set
	 */
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}

	/**
	 * Adds the given column.
	 * 
	 * @param column
	 *            The column
	 */
	public void addColumn(Column column) {
		if (column != null) {
			columns.add(column);
		}
	}

	/**
	 * Adds the given columns.
	 * 
	 * @param columns
	 *            The columns
	 */
	public void addColumns(Collection<Column> columns) {
		for (Column column : columns) {
			addColumn(column);
		}
	}


}
