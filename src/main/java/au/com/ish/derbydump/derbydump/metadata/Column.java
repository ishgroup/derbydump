package au.com.ish.derbydump.derbydump.metadata;

/**
 * Represents a column in a database table.
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
}
