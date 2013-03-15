package com.db.exporter.reader.impl;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.db.exporter.beans.Column;
import com.db.exporter.beans.MetaDataColumnDescriptor;
import com.db.exporter.beans.Table;

import com.db.exporter.beans.Database;

public class MetadataReader {
    
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
        columnsForColumn.add(new MetaDataColumnDescriptor("DATA_TYPE",      Types.INTEGER, new Integer(java.sql.Types.OTHER)));
        columnsForColumn.add(new MetaDataColumnDescriptor("NUM_PREC_RADIX", Types.INTEGER, new Integer(10)));
        columnsForColumn.add(new MetaDataColumnDescriptor("DECIMAL_DIGITS", Types.INTEGER, new Integer(0)));
        columnsForColumn.add(new MetaDataColumnDescriptor("COLUMN_SIZE",    Types.VARCHAR));
        columnsForColumn.add(new MetaDataColumnDescriptor("IS_NULLABLE",    Types.VARCHAR, "YES"));
        columnsForColumn.add(new MetaDataColumnDescriptor("REMARKS",        Types.VARCHAR));
    }
    
    public Database readDatabase(Connection conn)
    {
        Database database = new Database();
        database.setDatabaseName("mydatabase");
        try{
            
            DatabaseMetaData dmd = conn.getMetaData();
            ResultSet tables = dmd.getTables(null, null, null, new String[]{"TABLE"});
            while(tables.next()){
                Map values = readColumns(tables, _columnsForTable);
                Table table  = readTable(dmd, values);

                if (table != null)
                {
                    database.addTable(table);
                }
            }
        }
        catch(SQLException e){
            e.printStackTrace();
        }
        return database;
    }
    
    Map<String, Object> readColumns(ResultSet resultSet, List<MetaDataColumnDescriptor> columnDescriptors) throws SQLException
    {
        HashMap<String, Object> values = new HashMap<String, Object>();

        for (Iterator<MetaDataColumnDescriptor> it = columnDescriptors.iterator(); it.hasNext();)
        {
            MetaDataColumnDescriptor descriptor = it.next();
            values.put(descriptor.getName(), descriptor.readColumn(resultSet));
        }
        return values;
    }
    
    Table readTable(DatabaseMetaData metaData, Map values) throws SQLException
    {
        String tableName = (String)values.get("TABLE_NAME");
        Table  table     = null;
        
        if ((tableName != null) && (tableName.length() > 0))
        {
            table = new Table();
            table.setTableName(tableName);
            /*table.setType((String)values.get("TABLE_TYPE"));
            table.setCatalog((String)values.get("TABLE_CAT"));
            table.setSchema((String)values.get("TABLE_SCHEM"));
            table.setDescription((String)values.get("REMARKS"));*/

            table.addColumns(readColumns(metaData, tableName));
            /*table.addForeignKeys(readForeignKeys(metaData, tableName));
            table.addIndices(readIndices(metaData, tableName));

            Collection primaryKeys = readPrimaryKeyNames(metaData, tableName);

            for (Iterator it = primaryKeys.iterator(); it.hasNext();)
            {
                table.findColumn((String)it.next(), true).setPrimaryKey(true);
            }

            if (getPlatformInfo().isSystemIndicesReturned())
            {
                removeSystemIndices(metaData, table);
            }
*/        }
        return table;
    }
    
    List<Column> readColumns(DatabaseMetaData metaData, String tableName) throws SQLException
    {
        ResultSet columnData = null;
        try
        {
            columnData = metaData.getColumns(null, null,escapeForSearch(metaData, tableName), "%");
            List<Column> columns = new ArrayList<Column>();
            while (columnData.next())
            {
                Map values = readColumns(columnData, columnsForColumn);
                columns.add(readColumn(metaData, values));
            }
            columnData.close();
            return columns;
        }
        finally
        {
            //closeResultSet(columnData);
        }
    }
    
    Column readColumn(DatabaseMetaData metaData, Map values) throws SQLException
    {
        Column column = new Column();
        column.setColumnName((String)values.get("COLUMN_NAME"));
        column.setColumnDataType(((Integer)values.get("DATA_TYPE")).intValue());
        //column.setName((String)values.get("COLUMN_NAME"));
        /*column.setDefaultValue((String)values.get("COLUMN_DEF"));
        column.setTypeCode(((Integer)values.get("DATA_TYPE")).intValue());

        Integer precision = (Integer)values.get("NUM_PREC_RADIX");

        if (precision != null)
        {
            column.setPrecisionRadix(precision.intValue());
        }
*/
        /*String size = (String)values.get("COLUMN_SIZE");

        if (size == null)
        {
            size = (String)_defaultSizes.get(new Integer(column.getTypeCode()));
        }
        // we're setting the size after the precision and radix in case
        // the database prefers to return them in the size value
        column.setSize(size);

        Integer scale = (Integer)values.get("DECIMAL_DIGITS");

        if (scale != null)
        {
            // if there is a scale value, set it after the size (which probably did not contain
            // a scale specification)
            column.setScale(scale.intValue());
        }
        column.setRequired("NO".equalsIgnoreCase(((String)values.get("IS_NULLABLE")).trim()));

        String description = (String)values.get("REMARKS");

        if (!org.apache.ddlutils.util.StringUtilsExt.isEmpty(description))
        {
            column.setDescription(description);
        }
*/        return column;
    }
    
    public String escapeForSearch(DatabaseMetaData metaData, String literalString) throws SQLException
    {
        String escape = metaData.getSearchStringEscape();

        if (escape == "")
        {
            // No escape string, so nothing to do...
            return literalString;
        }
        else
        {
            // with Java 5, we would just use Matcher.quoteReplacement
            StringBuffer quotedEscape = new StringBuffer();

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
