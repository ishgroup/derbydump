package com.db.exporter.beans;

import java.util.List;

/**
 * This bean is an object representation of single table which is present in the
 * database.Just like any database table has certain attributes,i.e.name, columns,etc
 * this bean also has attributes.
 * 
 * @author Abhijeet
 * 
 */
public class Table {

	/**
	 * List of Columns present in the table.
	 */
	private List<Column> columns;
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

}
