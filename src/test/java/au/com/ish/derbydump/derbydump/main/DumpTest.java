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
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.log4j.Logger;
import org.junit.*;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.*;
import java.util.*;

import static junit.framework.Assert.*;
import static junit.framework.Assert.assertTrue;

/**
 * comprehensive test for the whole process
 */
@RunWith(Parameterized.class)
public class DumpTest {

	private static final Logger LOGGER = Logger.getLogger(DumpTest.class);

	private static final String RESOURCE_DATABASE_PATH = "memory:testdb";
	private static final String RESOURCE_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String RESOURCE_SCHEMA_NAME = "app";
	private static final String RESOURCE_DUMP_LOCATION = "./target/test.sql";
	private static final int RESOURCE_MAX_BUFFER_SIZE = 200;

	private DBConnectionManager db;

	private Configuration config;

	private String tableName;
	private String outputTableName;
	private boolean skipped;
	private String[] columns;
	private Object[] valuesToInsert;
	private String[] validOutputs;

	@Before
	public void setUp() throws Exception {
		config = Configuration.getConfiguration();
		config.setDerbyDbPath(RESOURCE_DATABASE_PATH);
		config.setDriverClassName(RESOURCE_DRIVER_NAME);
		config.setSchemaName(RESOURCE_SCHEMA_NAME);
		config.setBufferMaxSize(RESOURCE_MAX_BUFFER_SIZE);
		config.setOutputFilePath(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		db = new DBConnectionManager(config.getDerbyUrl().replace("create=false", "create=true"));
	}

	public DumpTest(String tableName, String outputTableName, String[] columns, Object[] valuesToInsert, String[] validOutputs, boolean skipped) {
		this.tableName = tableName;
		if (outputTableName == null) {
			this.outputTableName = tableName.toUpperCase();
		} else {
			this.outputTableName = outputTableName;
		}
		this.columns = columns;
		this.valuesToInsert = valuesToInsert;
		this.validOutputs = validOutputs;
		this.skipped = skipped;
	}

	@Parameterized.Parameters(name = "{0}")
	public static Collection<Object[]> setupTestMatrix() throws Exception {
		List<Object[]> result = new ArrayList<Object[]>();

		//testing numbers (BIGINT, DECIMAL, REAL, SMALLINT, INTEGER)
		{
			//standard set of numbers
			String[] columns = new String[] {"c1 BIGINT", "c2 DECIMAL(10,2)", "c3 REAL", "c4 SMALLINT", "c5 INTEGER"};
			Object[] row1 = new Object[] {new BigInteger("12"), new BigDecimal("12.12"), new Float("12.1"), Integer.valueOf(12), Integer.valueOf(24)};
			String validOutput1 = "(12,12.12,12.1,12,24),";
			Object[] row2 = new Object[] {new BigInteger("42"), new BigDecimal("42.12"), new Float("42.14"), Integer.valueOf(42), Integer.valueOf(64)};
			String validOutput2 = "(42,42.12,42.14,42,64),";
			Object[] row3 = new Object[] {new BigInteger("42"), new BigDecimal("42"), new Float("42"), Integer.valueOf(42), Integer.valueOf(64)};
			String validOutput3 = "(42,42.00,42.0,42,64),";
			Object[] row4 = new Object[] {new BigInteger("42"), new BigDecimal("42.1234"), new Float("42.1434"), Integer.valueOf(42), Integer.valueOf(64)};
			String validOutput4 = "(42,42.12,42.1434,42,64),";
			//test nulls
			Object[] row5 = new Object[] {null, null, null, null, null};
			String validOutput5 = "(NULL,NULL,0.0,NULL,NULL);";
			Object[] values = new Object[] {row1, row2, row3, row4, row5};
			String[] validOutput = new String[] {validOutput1, validOutput2, validOutput3, validOutput4, validOutput5};

			result.add(new Object[] {"testNumbers", null, columns, values, validOutput, false});
		}

		//testing strings
		{
			String[] columns = new String[] {"c1 VARCHAR(20)", "c2 VARCHAR(20)", "c3 VARCHAR(20)"};
			//test normal characters
			Object[] row1 = new Object[] {"123", "abc", "漢字"};
			String validOutput1 = "('123','abc','漢字'),";
			//test nulls
			Object[] row2 = new Object[] {"%", null, ""};
			String validOutput2 = "('%',NULL,''),";
			//test quotes and tabs
			Object[] row3 = new Object[] {"'test'", "\"test\"", "\t"};
			String validOutput3 = "('\\'test\\'','\"test\"','\\t'),";
			//test new line chars
			Object[] row4 = new Object[] {"\n", "\r", "\n\r"};
			String validOutput4 = "('\\n','\\r','\\n\\r');";

			Object[] values = new Object[] {row1, row2, row3, row4};
			String[] validOutput = new String[] {validOutput1, validOutput2, validOutput3, validOutput4};

			result.add(new Object[] {"testStrings", null, columns, values, validOutput, false});
		}

		//testing dates
		{
			String[] columns = new String[] {"c1 TIMESTAMP", "c2 TIMESTAMP"};
			// test standard dates
			Calendar c = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
			c.set(Calendar.YEAR, 2013);
			c.set(Calendar.MONTH, 5);
			c.set(Calendar.DAY_OF_MONTH, 6);
			c.set(Calendar.HOUR_OF_DAY, 11);
			c.set(Calendar.MINUTE, 10);
			c.set(Calendar.SECOND, 10);
			c.set(Calendar.MILLISECOND, 11);

			Calendar c2 = (Calendar) c.clone();
			c2.add(Calendar.DATE, -5000);

			Object[] row1 = new Object[] {c.getTime(), c2.getTime()};
			String validOutput1 = "('2013-06-06 21:10:10.011','1999-09-28 21:10:10.011'),";
			Object[] row2 = new Object[] {null, null};
			String validOutput2 = "(NULL,NULL);";
			Object[] values = new Object[] {row1, row2};
			String[] validOutput = new String[] {validOutput1, validOutput2};

			result.add(new Object[] {"testDates", null, columns, values, validOutput, false});
		}

		//testing CLOB
		{
			String[] columns = new String[] {"c1 CLOB"};
			Object[] row1 = new Object[] {"<clob value here>"};
			String validOutput1 = "('<clob value here>'),";
			Object[] row2 = new Object[] {null};
			String validOutput2 = "(NULL);";
			Object[] values = new Object[] {row1, row2};
			String[] validOutput = new String[] {validOutput1, validOutput2};

			result.add(new Object[] {"testClob", null, columns, values, validOutput, false});
		}

		//testing BLOB
		{
			String[] columns = new String[] {"c1 BLOB"};
			Object[] row1 = new Object[] {getTestImage()};
			String validOutput1 = "("+ Column.processBinaryData(IOUtils.toByteArray(getTestImage()))+"),";
			Object[] row2 = new Object[] {null};
			String validOutput2 = "(NULL);";
			Object[] values = new Object[] {row1, row2};
			String[] validOutput = new String[] {validOutput1, validOutput2};

			result.add(new Object[] {"testBlob", null, columns, values, validOutput, false});
		}

		//testing skipping table
		{
			String[] columns = new String[] {"c1 VARCHAR(5)"};
			Object[] row1 = new Object[] {"123"};
			String validOutput1 = "";
			Object[] row2 = new Object[] {null};
			String validOutput2 = "(NULL);";
			Object[] values = new Object[] {row1, row2};
			String[] validOutput = new String[] {validOutput1, validOutput2};

			result.add(new Object[] {"testSkip", null, columns, values, validOutput, true});
		}

		//testing renaming table
		{
			String[] columns = new String[] {"c1 VARCHAR(5)"};
			Object[] row1 = new Object[] {"123"};
			String validOutput1 = "('123'),";
			Object[] row2 = new Object[] {null};
			String validOutput2 = "(NULL);";
			Object[] values = new Object[] {row1, row2};
			String[] validOutput = new String[] {validOutput1, validOutput2};

			result.add(new Object[] {"testRename", "testRenameNew", columns, values, validOutput, false});
		}

		//testing empty table
		{
			String[] columns = new String[] {"c1 VARCHAR(5)"};
			Object[] values = new Object[] {new Object[] {}};
			String[] validOutput = new String[] {};

			result.add(new Object[] {"testEmptyTable", null, columns, values, validOutput, true});
		}


		return result;
	}

	private static  InputStream getTestImage() {
		 return Thread.currentThread().getContextClassLoader().getResourceAsStream("Penguins.jpg");
	}

	@Test
	public void theTest() throws SQLException {
		// Create table
		StringBuffer createTableBuffer = new StringBuffer();
		createTableBuffer.append("CREATE TABLE ");
		createTableBuffer.append(Configuration.getConfiguration().getSchemaName());
		createTableBuffer.append(".");
		createTableBuffer.append(tableName);
		createTableBuffer.append(" (");

		StringBuffer insertBuffer = new StringBuffer();
		insertBuffer.append("INSERT INTO ");
		insertBuffer.append(RESOURCE_SCHEMA_NAME);
		insertBuffer.append(".");
		insertBuffer.append(tableName);
		insertBuffer.append(" VALUES (");

		for (String col:columns) {
			createTableBuffer.append(col.toUpperCase());
			//String[] c = col.split(" ");
			//insertBuffer.append(c[0].toUpperCase().trim());
			insertBuffer.append("?");
			if (!columns[columns.length-1].equals(col)) {
				createTableBuffer.append(", ");
				insertBuffer.append(",");
			}
		}

		createTableBuffer.append(")");
		insertBuffer.append(")");


		config.setTableRewriteProperty("testSkip", "--exclude--");
		config.setTableRewriteProperty("testRename", "testRenameNew");


		Connection connection = db.createNewConnection();
		Statement statement = connection.createStatement();
		PreparedStatement ps = null;

		try {
			statement.execute(createTableBuffer.toString());
			connection.commit();
			//config.setTableRewriteProperty("TABLE2", "--exclude--");

			for (Object o:valuesToInsert) {
				Object[] vals = (Object[]) o;
				if (vals.length > 0) {
					ps = db.getConnection().prepareStatement(insertBuffer.toString());
					for (int i=0;i<vals.length;i++) {
						if (vals[i] instanceof InputStream) {
							ps.setBinaryStream(i + 1, (InputStream) vals[i]);
						} else {
							ps.setObject(i+1, vals[i]);
						}
					}
					ps.execute();
					connection.commit();
				}
			}

			File f = new File(RESOURCE_DUMP_LOCATION);
			if (f.exists()) {
				f.delete();
			}

			OutputThread output = new OutputThread();
			Thread writer = new Thread(output, "File_Writer");
			writer.start();

			new DatabaseReader(output);
			// Let the writer know that no more data is coming
			writer.interrupt();
			writer.join();

			// Now let's read the output and see what is in it
			List<String> lines = FileUtils.readLines(f);

			assertEquals("Missing foreign key operations", "SET FOREIGN_KEY_CHECKS = 0;", lines.get(0));
			assertEquals("Missing foreign key operations", "SET FOREIGN_KEY_CHECKS = 1;", lines.get(lines.size()-1));

			if (!skipped) {
				assertTrue("LOCK missing", lines.contains("LOCK TABLES `" + outputTableName + "` WRITE;"));
				assertTrue("UNLOCK missing",lines.contains("UNLOCK TABLES;"));

				int index =  lines.indexOf("LOCK TABLES `" + outputTableName + "` WRITE;");
				assertTrue("INSERT missing", lines.get(index+1).startsWith("INSERT INTO "+outputTableName));
				for (String s : validOutputs) {
					assertTrue("VALUES missing :"+s, lines.contains(s));
				};
			} else {
				assertTrue("LOCK missing", !lines.contains("LOCK TABLES `" + outputTableName + "` WRITE;"));
			}
		} catch (Exception e) {
			e.printStackTrace();
			fail("failed to create test data" + e.getMessage());
		} finally {
			if (ps != null) {
				ps.close();
			}
			statement.close();
			connection.close();
		}
	}

	@After
	public void cleanUp() throws Exception {
		db.getConnection().close();
	}
}
