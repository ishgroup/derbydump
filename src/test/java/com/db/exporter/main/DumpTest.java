package com.db.exporter.main;

import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.db.exporter.config.Configuration;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.utils.StringUtils;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;

public class DumpTest {

	private static final String TABLE_NAME = "DumperTest";
	private static final String RESOURCE_DATABASE_PATH = "memory:testdb";
	private static final String RESOURCE_DRIVER_NAME = "org.apache.derby.jdbc.EmbeddedDriver";
	private static final String RESOURCE_SCHEMA_NAME = "app";
	private static final String RESOURCE_DUMP_LOCATION = "./src/test/test.sql";
	private static final int RESOURCE_MAX_BUFFER_SIZE = 200;

	private static final String GOOD_QUERY = "LOCK TABLES `DUMPERTEST` WRITE;\nINSERT INTO DUMPERTEST (ID, DES, TIME, TYPE, LOCATION, ALERT) VALUES \n(1,'TestData','1970-01-01','TestType',10,10);\nUNLOCK TABLES;";

	private static Connection connection;
	private static Configuration config;

	@BeforeClass
	public static void setUp() throws Exception {
		String url = StringUtils.getDerbyUrl("memory:testdb", "", "");

		connection = DBConnectionManager.getConnection(url);
		config = Configuration.getConfiguration("", "", RESOURCE_DATABASE_PATH,
				RESOURCE_DRIVER_NAME, RESOURCE_SCHEMA_NAME,
				RESOURCE_MAX_BUFFER_SIZE,
				new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		String sql = "CREATE TABLE "
				+ Configuration.getConfiguration().getSchemaName()
				+ "."
				+ TABLE_NAME
				+ "(Id INTEGER NOT NULL,Des VARCHAR(25),Time DATE,Type VARCHAR(25),Location INTEGER,Alert INTEGER)";

		Statement statement = connection.createStatement();
		statement.execute(sql);
		connection.commit();
		statement.close();
		Thread.sleep(2000);
		PreparedStatement ps = connection.prepareStatement("INSERT INTO "
				+ config.getSchemaName() + "." + TABLE_NAME
				+ " VALUES (?, ?,?,?,?,?)");
		ps.setInt(1, 1);
		ps.setString(2, "TestData");
		ps.setDate(3, new Date(2000));
		ps.setString(4, "TestType");
		ps.setInt(5, 10);
		ps.setInt(6, 10);
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

		InputStreamReader r = new InputStreamReader(new FileInputStream(file));
		char[] buffer = new char[1024];
		while ((r.read(buffer, 0, 1024)) > -1) {
			sb.append(buffer);
		}

		assertTrue("Error creating dump: ", file.exists() && file.length() > 0);
		assertTrue("Wrong dump created", sb.toString().contains(GOOD_QUERY));

	}

	@AfterClass
	public static void cleanUp() throws Exception {
		new File(RESOURCE_DUMP_LOCATION).delete();
		connection.close();
	}
}
