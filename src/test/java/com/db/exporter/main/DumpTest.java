package com.db.exporter.main;

import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.db.exporter.config.Configuration;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;

public class DumpTest {

	private static final long TIMESTAMP_THRESHHOLD = 20 * 1000;
	private static final String TABLE_NAME = "DumperTest";
	private static final String RESOURCE_NAME = "Penguins.jpg";
	private static Connection connection;

	@BeforeClass
	public static void setUp() throws Exception {
		connection = DBConnectionManager.getConnection();

		String sql = "CREATE TABLE "
				+ Configuration.getConfiguration().getSchemaName() + "."
				+ TABLE_NAME + "(id INT, pic blob(16M))";

		InputStream fin = DumpTest.class.getClassLoader().getResourceAsStream(
				RESOURCE_NAME);
		;

		Statement statement = connection.createStatement();
		statement.execute(sql);
		connection.commit();
		statement.close();
		Thread.sleep(2000);
		PreparedStatement ps = connection.prepareStatement("INSERT INTO "
				+ Configuration.getConfiguration().getSchemaName() + "."
				+ TABLE_NAME + " VALUES (?, ?)");
		ps.setInt(1, 1477);
		ps.setBinaryStream(2, fin);
		ps.execute();
		connection.commit();
		ps.close();
		connection.close();
	}

	@Test
	public void test() throws Exception {
		Thread reader = new Thread(new DatabaseReader(
				Configuration.getConfiguration(),
				BufferManager.getBufferInstance()), "Database_reader");
		Thread writer = new Thread(new FileWriter(
				Configuration.getConfiguration(),
				BufferManager.getBufferInstance()), "File_Writer");

		reader.start();
		writer.start();

		Thread.sleep(2000);
		File file = new File(Configuration.getConfiguration().getDumpFilePath());
		assertTrue("Error creating dump: ", file.exists() && file.length() > 0
		/*
		 * && System.currentTimeMillis() - file.lastModified() <
		 * TIMESTAMP_THRESHHOLD
		 */);
	}

	@AfterClass
	public static void cleanUp() throws Exception {
		connection = DBConnectionManager.getConnection();
		Statement statement = connection.createStatement();
		statement.execute("DROP TABLE "
				+ Configuration.getConfiguration().getSchemaName() + "."
				+ TABLE_NAME);
		connection.commit();
		statement.close();
		connection.close();
	}
}
