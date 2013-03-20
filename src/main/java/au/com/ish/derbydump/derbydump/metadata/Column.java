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
