package com.db.exporter.beans;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
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
	 * Number of columns present in the table
	 */
	private int numOfColumns;
	/**
	 * Number of rows present in the table
	 */
	private int numOfRows;

	/**
	 * @return the columns
	 */
	public List<Column> getColumns() {
		return columns;
	}

	/**
	 * @param columns
	 *            the columns to set
	 */
	public void setColumns(List<Column> columns) {
		this.columns = columns;
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
	 * @return the numOfColumns
	 */
	public int getNumOfColumns() {
		return numOfColumns;
	}

	/**
	 * @param numOfColumns
	 *            the numOfColumns to set
	 */
	public void setNumOfColumns(int numOfColumns) {
		this.numOfColumns = numOfColumns;
	}

	/**
	 * @return the numOfRows
	 */
	public int getNumOfRows() {
		return numOfRows;
	}

	/**
	 * @param numOfRows
	 *            the numOfRows to set
	 */
	public void setNumOfRows(int numOfRows) {
		this.numOfRows = numOfRows;
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

	public void addColumn(int idx, Column column) {
		if (column != null) {
			columns.add(idx, column);
		}
	}

	public void addColumn(Column previousColumn, Column column) {
		if (column != null) {
			if (previousColumn == null) {
				columns.add(0, column);
			} else {
				columns.add(columns.indexOf(previousColumn), column);
			}
		}
	}

	/**
	 * Adds the given columns.
	 * 
	 * @param columns
	 *            The columns
	 */
	public void addColumns(Collection columns) {
		for (Iterator it = columns.iterator(); it.hasNext();) {
			addColumn((Column) it.next());
		}
	}

}
