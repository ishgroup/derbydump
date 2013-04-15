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
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Types;

import static junit.framework.Assert.*;

public class DerbyDumpTest {

	private static final Logger LOGGER = Logger.getLogger(DerbyDumpTest.class);

	private static final String TABLE_NAME = "DumperTest";
	private static final String RESOURCE_DATABASE_PATH = "memory:testdbOld";
	private static final String RESOURCE_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String RESOURCE_SCHEMA_NAME = "app";
	private static final String RESOURCE_DUMP_LOCATION = "./target/test.sql";
	private static final int RESOURCE_MAX_BUFFER_SIZE = 200;

	private static DBConnectionManager db;

	private static String BIG_CLOB;

	@BeforeClass
	public static void setUp() throws Exception {
		Configuration config = Configuration.getConfiguration();
		config.setDerbyDbPath(RESOURCE_DATABASE_PATH);
		config.setDriverClassName(RESOURCE_DRIVER_NAME);
		config.setSchemaName(RESOURCE_SCHEMA_NAME);
		config.setBufferMaxSize(RESOURCE_MAX_BUFFER_SIZE);
		config.setOutputFilePath(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		db = new DBConnectionManager(config.getDerbyUrl().replace("create=false", "create=true"));

		String sql = "CREATE TABLE " + RESOURCE_SCHEMA_NAME + ".DumperTest"
				+ "(Id INTEGER NOT NULL,Des VARCHAR(25),Time DATE,nullTime TIMESTAMP, Type VARCHAR(25),Location INTEGER,Alert INTEGER, clobData CLOB(32000))";

		db.getConnection().createStatement().execute(sql);
		db.getConnection().commit();


		PreparedStatement ps = db.getConnection().prepareStatement("INSERT INTO " + RESOURCE_SCHEMA_NAME + ".DumperTest VALUES (?,?,?,?,?,?,?,?)");
		ps.setInt(1, 1);
		ps.setString(2, "TestData");
		ps.setDate(3, new Date(2000));
		//Test for null TIMESTAMP
		ps.setTimestamp(4, null);
		//The below will make sure that chinese characters e.g. UTF-8 encoded streams are properly read. 
		ps.setString(5, "漢字");
		ps.setInt(6, 10);
		ps.setInt(7, 10);

		//Test for CLOB data
		StringBuilder sb = new StringBuilder();
        String base = "<SampleClobData>";
        for (int i = 0; i < 1000; i++) {
	        sb.append(base);
        }
		BIG_CLOB = sb.toString();

        ps.setClob(8, new StringReader(BIG_CLOB), BIG_CLOB.length());
		ps.execute();
		db.getConnection().commit();
		ps.close();


		// Create table to exclude
		sql = "CREATE TABLE " + Configuration.getConfiguration().getSchemaName() + ".TABLE2 (Id INTEGER)";
		db.getConnection().createStatement().execute(sql);
		db.getConnection().commit();
		config.setTableRewriteProperty("TABLE2", "--exclude--");
		ps = db.getConnection().prepareStatement("INSERT INTO " + RESOURCE_SCHEMA_NAME + ".TABLE2 VALUES (?)");
		ps.setInt(1, 1);
		ps.execute();
		db.getConnection().commit();
		ps.close();


		// Create table to rename
		sql = "CREATE TABLE " + Configuration.getConfiguration().getSchemaName() + ".TABLE3 (Id BIGINT)";
		db.getConnection().createStatement().execute(sql);
		db.getConnection().commit();
		config.setTableRewriteProperty("TABLE3", "TABLE3New");
		ps = db.getConnection().prepareStatement("INSERT INTO " + RESOURCE_SCHEMA_NAME + ".TABLE3 VALUES (?)");
		ps.setLong(1, 1);
		ps.execute();
		ps.setNull(1, Types.BIGINT);
		ps.execute();
		db.getConnection().commit();
		ps.close();


		// Create table with no data
		sql = "CREATE TABLE " + Configuration.getConfiguration().getSchemaName() + ".TABLE4 (Id INTEGER)";
		db.getConnection().createStatement().execute(sql);
		db.getConnection().commit();

		
		// Create table with blob
		sql = "CREATE TABLE " + RESOURCE_SCHEMA_NAME + ".TABLE5 (data BLOB)";
		db.getConnection().createStatement().execute(sql);
		db.getConnection().commit();
		InputStream penguins = DerbyDumpTest.class.getResourceAsStream("Penguins.jpg");
		ps = db.getConnection().prepareStatement("INSERT INTO " + RESOURCE_SCHEMA_NAME + ".TABLE5 VALUES (?)");
		ps.setBinaryStream(1, penguins);
		ps.execute();
		db.getConnection().commit();
		ps.close();
	}

	@Test
	public void test() throws Exception {

		OutputThread output = new OutputThread();
		Thread writer = new Thread(output, "File_Writer");
		writer.start();

		new DatabaseReader(output);
		// Let the writer know that no more data is coming
		writer.interrupt();
		writer.join();

		// Now let's read the output and see what is in it
		BufferedReader r = new BufferedReader(new InputStreamReader(new FileInputStream(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath()),"UTF-8"));
		StringBuilder data = new StringBuilder();
		try {
			String line = r.readLine();
			while (line != null) {
				data.append(line);
				data.append("\n");
				line = r.readLine();
			}
		} finally {
			r.close();
		}

		assertTrue("Wrong dump created: LOCK missing", data.toString().contains("LOCK TABLES `DUMPERTEST` WRITE"));
		assertTrue("Wrong dump created: INSERT missing", data.toString().contains("INSERT INTO DUMPERTEST (ID,DES,TIME,NULLTIME,TYPE,LOCATION,ALERT,CLOBDATA) VALUES"));
		assertTrue("Wrong dump created: VALUES missing", data.toString().contains("1,'TestData','1970-01-01',NULL,'漢字'"));
		assertTrue("Wrong dump created: CLOB", data.toString().contains(BIG_CLOB));

		assertFalse("Wrong dump created: table should have been excluded", data.toString().contains("LOCK TABLES `TABLE2` WRITE"));
		assertFalse("Wrong dump created: table was not properly renamed", data.toString().contains("LOCK TABLES `TABLE3` WRITE"));
		assertTrue("Wrong dump created: table was not properly renamed", data.toString().contains("LOCK TABLES `TABLE3New` WRITE"));

		assertFalse("Wrong dump created: table has no data", data.toString().contains("LOCK TABLES `TABLE4` WRITE"));

		assertFalse("Wrong dump created: null BIGINT not properly handled", data.toString().contains("(0)"));
	}
	
	@AfterClass
	public static void cleanUp() throws Exception {
		db.getConnection().close();
	}
}
