package com.db.exporter.beans;

import java.util.List;
import java.util.Map;

public class Database {

	private String databaseName;
	private List<Table> tables;
	private Map<String, List<String>> dataMap;

	/**
	 * @return the tables
	 */
	public List<Table> getTables() {
		return tables;
	}

	/**
	 * @param tables the tables to set
	 */
	public void setTables(List<Table> tables) {
		this.tables = tables;
	}

	/**
	 * @return the databaseName
	 */
	public String getDatabaseName() {
		return databaseName;
	}

	/**
	 * @param databaseName the databaseName to set
	 */
	public void setDatabaseName(String databaseName) {
		this.databaseName = databaseName;
	}

	/**
	 * @return the dataMap
	 */
	public Map<String, List<String>> getDataMap() {
		return dataMap;
	}

	/**
	 * @param dataMap the dataMap to set
	 */
	public void setDataMap(Map<String, List<String>> dataMap) {
		this.dataMap = dataMap;
	}
	
	
}
