package au.com.ish.derbydump.derbydump.main;

import java.io.File;
import java.sql.*;
import java.util.List;

import au.com.ish.derbydump.derbydump.config.Configuration;
import au.com.ish.derbydump.derbydump.config.DBConnectionManager;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.fail;

public class BlobDumpTest {


	private Configuration config;
	private File actualDump = new File("./build/outputs/actualDump.sql");
	private File expectedDump = new File("./src/test/resources/expectedDump.sql");

	@Before
	public void setUp() throws Exception {
		if (actualDump.exists()) {
			actualDump.delete();
		}
		actualDump.mkdirs();

		config = Configuration.getConfiguration();
		config.setDerbyDbPath(DumpTest.RESOURCE_DATABASE_PATH);
		config.setDriverClassName(DumpTest.RESOURCE_DRIVER_NAME);
		config.setSchemaName(DumpTest.RESOURCE_SCHEMA_NAME);
		config.setBufferMaxSize(DumpTest.RESOURCE_MAX_BUFFER_SIZE);
		config.setOutputFilePath(actualDump.getCanonicalPath());
		config.setTruncateTables(false);

		System.out.println("db "+config.getDerbyUrl().replace("create=false", "create=true"));
	}

	@After
	public void tearDown() throws Exception {
		try {
			new DBConnectionManager("jdbc:derby:"+config.getDerbyDbPath()+";drop=true");
		} catch (SQLNonTransientConnectionException e) {
			//the db was dropped
		}
	}

	@Test
	public void theDumpTest() throws Exception {

		DBConnectionManager db = new DBConnectionManager(config.getDerbyUrl().replace("create=false", "create=true"));

		// Create table
		String createTable = "CREATE TABLE app.test (data BLOB)";

		String insertString = "INSERT INTO app.test VALUES (?)";

		Connection connection = db.createNewConnection();
		Statement statement = connection.createStatement();
		PreparedStatement ps = null;
		try {

			statement.execute("SET SCHEMA app");
			connection.commit();

			statement.execute(createTable);
			connection.commit();

			ps = db.getConnection().prepareStatement(insertString);

			ps.setBinaryStream(1, Thread.currentThread().getContextClassLoader().getResourceAsStream("Penguins.jpg"));
			ps.execute();
			connection.commit();

			OutputThread output = new OutputThread();
			Thread writer = new Thread(output, "File_Writer");
			writer.start();

			new DatabaseReader(output);
			// Let the writer know that no more data is coming
			writer.interrupt();
			writer.join();

			// Now let's read the output and see what is in it
			String md5ActualDump = DigestUtils.md5Hex(FileUtils.readFileToByteArray(actualDump));
			String md5ExpectedDump = DigestUtils.md5Hex(FileUtils.readFileToByteArray(expectedDump));

			//test below are redundant, but they help to debug problems.
			List<String> actualLines = FileUtils.readLines(actualDump);
			List<String> expectedLines = FileUtils.readLines(expectedDump);

			assertEquals(expectedLines.size() , actualLines.size());

			for (int i=0;i<actualLines.size();i++) {
				assertEquals(expectedLines.get(i), actualLines.get(i));
				assertEquals("\n"+expectedLines.get(i)+" vs\n"+ actualLines.get(i),DigestUtils.md5Hex(expectedLines.get(i)), DigestUtils.md5Hex(actualLines.get(i)));
			}


			// this is the most important test
			assertEquals(md5ExpectedDump , md5ActualDump);


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
}
