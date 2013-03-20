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
