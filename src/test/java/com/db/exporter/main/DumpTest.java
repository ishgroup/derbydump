package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.StringUtils;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;

import static junit.framework.Assert.assertTrue;

public class DumpTest {

	private static final String TABLE_NAME = "DumperTest";
	private static final String RESOURCE_DATABASE_PATH = "memory:testdb";
	private static final String RESOURCE_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String RESOURCE_SCHEMA_NAME = "app";
	private static final String RESOURCE_DUMP_LOCATION = "./target/test.sql";
	private static final int RESOURCE_MAX_BUFFER_SIZE = 200;

	private static final String GOOD_QUERY = "LOCK TABLES `DUMPERTEST` WRITE;\nINSERT INTO DUMPERTEST (ID, DES, TIME, NULLTIME, TYPE, LOCATION, ALERT) VALUES \n(1,'TestData','1970-01-01',null,'漢字',10,10);\nUNLOCK TABLES;";

	private static Connection connection;
	private static Configuration config;

	@BeforeClass
	public static void setUp() throws Exception {
		String url = StringUtils.getDerbyUrl("memory:testdb", "", "");
		url.replace("create=false", "create=true");

		connection = DBConnectionManager.getConnection(url);
		config = Configuration.getConfiguration("", "", RESOURCE_DATABASE_PATH,
				RESOURCE_DRIVER_NAME, RESOURCE_SCHEMA_NAME,
				RESOURCE_MAX_BUFFER_SIZE,
				new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		String sql = "CREATE TABLE "
				+ Configuration.getConfiguration().getSchemaName()
				+ "."
				+ TABLE_NAME
				+ "(Id INTEGER NOT NULL,Des VARCHAR(25),Time DATE,nullTime TIMESTAMP, Type VARCHAR(25),Location INTEGER,Alert INTEGER)";

		Statement statement = connection.createStatement();
		statement.execute(sql);
		connection.commit();
		statement.close();
		Thread.sleep(2000);
		PreparedStatement ps = connection.prepareStatement("INSERT INTO "
				+ config.getSchemaName() + "." + TABLE_NAME
				+ " VALUES (?,?,?,?,?,?,?)");
		ps.setInt(1, 1);
		ps.setString(2, "TestData");
		ps.setDate(3, new Date(2000));
		//Test for null TIMESTAMP
		ps.setTimestamp(4, null);
		//The below will make sure that chinese characters e.g. UTF-8 encoded streams are properly read. 
		ps.setString(5, "漢字");
		ps.setInt(6, 10);
		ps.setInt(7, 10);
		ps.execute();
		connection.commit();
		ps.close();
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
		File file = new File(config.getDumpFilePath());

		StringBuilder sb = new StringBuilder(1000);

		InputStreamReader r = new InputStreamReader(new FileInputStream(file), "UTF-8");
		char[] buffer = new char[1024];
		while ((r.read(buffer, 0, 1024)) > -1) {
			sb.append(buffer);
		}

		assertTrue("Error creating dump: ", file.exists() && file.length() > 0);
		assertTrue("Wrong dump created", sb.toString().contains(GOOD_QUERY));
		r.close();
	}

	@AfterClass
	public static void cleanUp() throws Exception {
		new File(RESOURCE_DUMP_LOCATION).delete();
		connection.close();
	}
}
