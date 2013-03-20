package au.com.ish.derbydump.derbydump.metadata;

import java.util.ArrayList;
import java.util.List;


/**
 * Represents a database. 
 *
 */

public class Database {

	private String databaseName;
	private List<Table> tables = new ArrayList<Table>();

	/**
	 * @return the tables
	 */
	public List<Table> getTables() {
		return tables;
	}
	
	public void addTable(Table table)
    {
        if (table != null)
        {
            tables.add(table);
        }
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

}
