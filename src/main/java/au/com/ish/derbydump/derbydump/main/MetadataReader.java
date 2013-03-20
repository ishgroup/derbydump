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

import au.com.ish.derbydump.derbydump.metadata.Column;
import au.com.ish.derbydump.derbydump.metadata.Database;
import au.com.ish.derbydump.derbydump.metadata.MetaDataColumnDescriptor;
import au.com.ish.derbydump.derbydump.metadata.Table;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.*;
import java.util.regex.Pattern;

public class MetadataReader {
	private static final Logger LOGGER = Logger.getLogger(MetadataReader.class);

    private final Pattern searchStringPattern = Pattern.compile("[_%]");
    private static final List<MetaDataColumnDescriptor> columnsForColumn;
    private static final List<MetaDataColumnDescriptor> _columnsForTable;
    
    static{
        _columnsForTable = new ArrayList<MetaDataColumnDescriptor>();
        _columnsForTable.add(new MetaDataColumnDescriptor("TABLE_NAME",  Types.VARCHAR));
        _columnsForTable.add(new MetaDataColumnDescriptor("TABLE_TYPE",  Types.VARCHAR, "UNKNOWN"));
        _columnsForTable.add(new MetaDataColumnDescriptor("TABLE_CAT",   Types.VARCHAR));
        _columnsForTable.add(new MetaDataColumnDescriptor("TABLE_SCHEM", Types.VARCHAR));
        _columnsForTable.add(new MetaDataColumnDescriptor("REMARKS",     Types.VARCHAR));
    }
    static{
        columnsForColumn = new ArrayList<MetaDataColumnDescriptor>();

        columnsForColumn.add(new MetaDataColumnDescriptor("COLUMN_DEF",     Types.VARCHAR));
        // we're also reading the table name so that a model reader impl can filter manually
        columnsForColumn.add(new MetaDataColumnDescriptor("TABLE_NAME",     Types.VARCHAR));
        columnsForColumn.add(new MetaDataColumnDescriptor("COLUMN_NAME",    Types.VARCHAR));
        columnsForColumn.add(new MetaDataColumnDescriptor("DATA_TYPE",      Types.INTEGER, Types.OTHER));
        columnsForColumn.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", Types.INTEGER, 10));
        columnsForColumn.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", Types.INTEGER, 0));
        columnsForColumn.add(new MetaDataColumnDescriptor("COLUMN_SIZE",    Types.VARCHAR));
        columnsForColumn.add(new MetaDataColumnDescriptor("IS_NULLABLE",    Types.VARCHAR, "YES"));
        columnsForColumn.add(new MetaDataColumnDescriptor("REMARKS",        Types.VARCHAR));
    }
    
    public Database readDatabase(Connection conn) {
        Database database = new Database();
        database.setDatabaseName("mydatabase");
        try{
            
            DatabaseMetaData dmd = conn.getMetaData();
            ResultSet tables = dmd.getTables(null, null, null, new String[]{"TABLE"});
            while (tables.next()) {
                Map values = readMetaData(tables, _columnsForTable);
                Table table  = readTable(dmd, values);
				LOGGER.debug("Found table: " + table.getTableName());

				database.addTable(table);
            }
        }
        catch(SQLException e){
            LOGGER.error(e);
        }
        return database;
    }
    
    Map<String, Object> readMetaData(ResultSet resultSet, List<MetaDataColumnDescriptor> columnDescriptors) throws SQLException {
        HashMap<String, Object> values = new HashMap<String, Object>();

	    for (MetaDataColumnDescriptor descriptor : columnDescriptors) {
		    values.put(descriptor.getName(), descriptor.readColumn(resultSet));
	    }
        return values;
    }
    
    Table readTable(DatabaseMetaData metaData, Map values) throws SQLException {
        String tableName = (String)values.get("TABLE_NAME");
        Table table = null;
        
        if ((tableName != null) && (tableName.length() > 0)) {
            table = new Table();
            table.setTableName(tableName);
	        table.addColumns(readColumns(metaData, tableName));
        }
        return table;
    }
    
    List<Column> readColumns(DatabaseMetaData metaData, String tableName) throws SQLException {
        ResultSet columnData = metaData.getColumns(null, null, escapeForSearch(metaData, tableName), "%");
	    List<Column> columns = new ArrayList<Column>();

	    while (columnData.next()) {
	        Map values = readMetaData(columnData, columnsForColumn);
		    Column column = new Column();
		    column.setColumnName((String)values.get("COLUMN_NAME"));
		    column.setColumnDataType((Integer) values.get("DATA_TYPE"));
	        columns.add(column);
	    }
	    columnData.close();
	    return columns;
    }

	/**
	 * This comes from https://svn.apache.org/repos/asf/db/ddlutils/trunk/src/main/java/org/apache/ddlutils/platform/DatabaseMetaDataWrapper.java
	 *
	 * @param metaData
	 * @param literalString
	 * @return
	 * @throws SQLException
	 */
    String escapeForSearch(DatabaseMetaData metaData, String literalString) throws SQLException {
        String escape = metaData.getSearchStringEscape();

        if (escape.equals("")) {
            // No escape string, so nothing to do...
            return literalString;

        } else {
            // with Java 5, we would just use Matcher.quoteReplacement
            StringBuilder quotedEscape = new StringBuilder();

            for (int idx = 0; idx < escape.length(); idx++)
            {
                char c = escape.charAt(idx);

                switch (c)
                {
                    case '\\':
                        quotedEscape.append("\\\\");
                        break;
                    case '$':
                        quotedEscape.append("\\$");
                        break;
                    default:
                        quotedEscape.append(c);
                }
            }
            quotedEscape.append("$0");

            return searchStringPattern.matcher(literalString).replaceAll(quotedEscape.toString());
        }
    }

}
