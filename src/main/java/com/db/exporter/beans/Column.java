package com.db.exporter.beans;

/**
 * This class is an object representation of a single column present in the table. Just like 
 * any column in the database table has certain attributes,i.e. name, data type, etc., this class 
 * also has same attributes.
 * @author Abhijeet
 *
 */
public class Column {
	
	/**
	 * Name of the column
	 */
	private String columnName;
	/**
	 * Data type of the column
	 */
	private String columnDataType;
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
	public String getColumnDataType() {
		return columnDataType;
	}
	/**
	 * @param columnDataType the columnDataType to set
	 */
	public void setColumnDataType(String columnDataType) {
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
	
	

}
