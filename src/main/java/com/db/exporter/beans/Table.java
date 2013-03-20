package com.db.exporter.beans;

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
