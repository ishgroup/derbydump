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

package au.com.ish.derbydump.derbydump.main;

import au.com.ish.derbydump.derbydump.config.Configuration;
import au.com.ish.derbydump.derbydump.config.DBConnectionManager;
import au.com.ish.derbydump.derbydump.metadata.Column;
import au.com.ish.derbydump.derbydump.metadata.Database;
import au.com.ish.derbydump.derbydump.metadata.Table;
import org.apache.log4j.Logger;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * Logical module representing a reader/producer which reads from a database and
 * writes to a buffer.
 */
public class DatabaseReader {

	private final static int MAX_ALLOWED_ROWS = 100;
	private static final Logger LOGGER = Logger.getLogger(DatabaseReader.class);
	private final OutputThread output;

	private Configuration config;

	public DatabaseReader(OutputThread output) {
		this.output = output;
		config = Configuration.getConfiguration();

		LOGGER.debug("Database reader initializing...");
		readMetaData(config.getSchemaName());
	}

	void readMetaData(String schema) {
		// getting the connection
		DBConnectionManager db;
		try {
			db = new DBConnectionManager(config.getDerbyUrl());
		} catch (Exception e) {
			LOGGER.error("Could not establish Database connection.", e);
			return;
		}
		// creating a skeleton of tables and columns present in the database
		MetadataReader metadata = new MetadataReader();
		LOGGER.debug("Resolving database structure...");
		Database database = metadata.readDatabase(db.getConnection());
		getInternalData(database.getTables(), db.getConnection(), schema);

		try {
			db.getConnection().close();
		} catch (SQLException e) {
			LOGGER.error("Could not close database connection :" + e.getErrorCode() + " - " + e.getMessage());
		}

	}

	/**
	 * Read data from each {@link Table} and add it to
	 * the output.
	 * 
	 * @param tables A list of tables to read from
	 * @param connection The database connection used to fetch the data
	 * @param schema The name of the schema we are using
	 */
	private void getInternalData(List<Table> tables, Connection connection, String schema) {
		LOGGER.debug("Fetching database data...");

		output.add("SET FOREIGN_KEY_CHECKS = 0;\n");

		for (Table table : tables) {
			if (!table.isExcluded()) {
				List<Column> columns = table.getColumns();
				LOGGER.info("Table " + table.getTableName() + "...\n");

				try {
					Statement statement = connection.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);
					ResultSet dataRows = statement.executeQuery(table.getSelectQuery(schema));
					int rowCount = 0;

					if (dataRows.first()) { // check that we have at least one row
						dataRows.beforeFirst();

						output.add("LOCK TABLES `" + table.getTableName() + "` WRITE;\n");
						if (config.getTruncateTables()) {
							output.add("TRUNCATE TABLE " + table.getTableName() + ";\n");
						}
						output.add(table.getInsertSQL());
						output.add("\n");

						while (dataRows.next()) {

							output.add("(");
							
							boolean firstColumn = true;
							for (Column column : columns) {
								if (firstColumn) {
									firstColumn = false;
								} else {
									output.add(",");
								}
								output.add(column.toString(dataRows));
							}
							rowCount++;
							output.add(")");
							
							
							if (!dataRows.isLast()) {
								if (rowCount % MAX_ALLOWED_ROWS == 0) {
									output.add(";\n");
									output.add(table.getInsertSQL());
									output.add("\n");
								} else {
									output.add(",\n");									
								}
							}
						}

						output.add(";\n");
						output.add("UNLOCK TABLES;\n");

						dataRows.close();
						statement.close();

					}

				} catch (SQLException e) {
					LOGGER.error("Error: " + e.getErrorCode() + " - " + e.getMessage());
				}
			}
		}
		output.add("SET FOREIGN_KEY_CHECKS = 1;");
		LOGGER.debug("Reading done.");
	}
}