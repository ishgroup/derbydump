package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.StringUtils;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;
import org.apache.commons.codec.CharEncoding;
import org.apache.commons.codec.binary.Hex;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.*;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertTrue;

public class DerbyDumpTest {

	private static final Logger LOGGER = Logger.getLogger(DerbyDumpTest.class);

	private static final String TABLE_NAME = "DumperTest";
	private static final String RESOURCE_DATABASE_PATH = "memory:testdb";
	private static final String RESOURCE_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String RESOURCE_SCHEMA_NAME = "app";
	private static final String RESOURCE_DUMP_LOCATION = "./target/test.sql";
	private static final int RESOURCE_MAX_BUFFER_SIZE = 200;

	private static StringBuilder GOOD_QUERY = new StringBuilder("LOCK TABLES `DUMPERTEST` WRITE;\nINSERT INTO DUMPERTEST (ID, DES, TIME, NULLTIME, TYPE, LOCATION, ALERT, CLOBDATA) VALUES \n(1,'TestData','1970-01-01',null,'漢字',10,10");

	private static Connection connection;
	private static Configuration config;

	@BeforeClass
	public static void setUp() throws Exception {
		String url = StringUtils.getDerbyUrl("memory:testdb", "", "");
		url = url.replace("create=false", "create=true");

		config = Configuration.getConfiguration();
		config.setDerbyDbPath(RESOURCE_DATABASE_PATH);
		config.setDriverClassName(RESOURCE_DRIVER_NAME);
		config.setSchemaName(RESOURCE_SCHEMA_NAME);
		config.setBufferMaxSize(RESOURCE_MAX_BUFFER_SIZE);
		config.setOutputFilePath(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		connection = DBConnectionManager.getConnection(url);

		String sql = "CREATE TABLE "
				+ Configuration.getConfiguration().getSchemaName()
				+ "."
				+ TABLE_NAME
				+ "(Id INTEGER NOT NULL,Des VARCHAR(25),Time DATE,nullTime TIMESTAMP, Type VARCHAR(25),Location INTEGER,Alert INTEGER, clobData CLOB(32000))";

		Statement statement = connection.createStatement();
		statement.execute(sql);
		connection.commit();
		statement.close();
		Thread.sleep(2000);
		PreparedStatement ps = connection.prepareStatement("INSERT INTO "
				+ config.getSchemaName() + "." + TABLE_NAME
				+ " VALUES (?,?,?,?,?,?,?,?)");
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
		StringBuffer sb = new StringBuffer ();
        String base = "<SampleClobData>";
        for (int i = 0; i < 1000; i++) {
            sb.append (base);
        }
        //insert a large enough data to ensure stream is created in dvd
        ps.setClob(8, new StringReader(sb.toString()), sb.length());
        //ps.setCharacterStream (8, new StringReader(sb.toString()), sb.length());
		ps.execute();
		connection.commit();
		ps.close();
		GOOD_QUERY.append(",'"+sb+"'");
		GOOD_QUERY.append(");\nUNLOCK TABLES;");
	}

	@Test
	public void test() throws Exception {
		Thread reader = new Thread(new DatabaseReader(config,
				BufferManager.getBufferInstance()), "Database_reader");
		Thread writer = new Thread(new FileWriter(config,
				BufferManager.getBufferInstance()), "File_Writer");

		reader.start();
		writer.start();

		Thread.sleep(2000);
		File file = new File(config.getOutputFilePath());

		StringBuilder sb = new StringBuilder();

		InputStreamReader r = new InputStreamReader(new FileInputStream(file), "UTF-8");
		char[] buffer = new char[1024];
		while ((r.read(buffer, 0, 1024)) > -1) {
			sb.append(buffer);
		}

		assertTrue("Error creating dump: ", file.exists() && file.length() > 0);
		assertTrue("Wrong dump created", sb.toString().contains(GOOD_QUERY));
		r.close();
	}
	
	@Test
	public void testEscapeQuotes(){
	    String positive_test1 = "Test for escapaing the quotes (here's it goes)";
	    String positive_test2 = "Test for escapaing the double quotes (here''s it goes)";
	    
	    assertEquals(positive_test1, "Test for escapaing the quotes (here''s it goes)", StringUtils.escapeQuotes(positive_test1));
	    assertEquals(positive_test2, "Test for escapaing the double quotes (here''''s it goes)", StringUtils.escapeQuotes(positive_test2));
   }
	
	@Test
	public void testHexUtils(){
		Hex hexEncoder = new Hex(CharEncoding.UTF_8);

		try {
			byte[] test1 = "Hex String ==;90%$#@^ Byte Array".getBytes(CharEncoding.UTF_8);
			byte[] test1_expected = "48657820537472696e67203d3d3b3930252423405e2042797465204172726179".getBytes(CharEncoding.UTF_8);

			byte[] test1_output = hexEncoder.encode(test1);
			LOGGER.debug("Hex output: " + new String(test1_output));
			assertEquals("failure In converting byte to HEX", new String(test1_expected).toUpperCase(), new String(test1_output).toUpperCase());

			byte[] test2 = "中國全國人大、政協「兩會」綜合報導 Read more:".getBytes(CharEncoding.UTF_8);
			byte[] test2_expected = "E4B8ADE59C8BE585A8E59C8BE4BABAE5A4A7E38081E694BFE58D94E3808CE585A9E69C83E3808DE7B69CE59088E5A0B1E5B08E2052656164206D6F72653A".getBytes(CharEncoding.UTF_8);

			byte[] test2_output = hexEncoder.encode(test2);
		    assertEquals("failure In converting byte to HEX For Chinese", new String(test2_expected).toUpperCase(), new String(test2_output).toUpperCase());

		} catch (UnsupportedEncodingException ignored) {}
	}
	
	@AfterClass
	public static void cleanUp() throws Exception {
		new File(RESOURCE_DUMP_LOCATION).delete();
		connection.close();
	}
}
