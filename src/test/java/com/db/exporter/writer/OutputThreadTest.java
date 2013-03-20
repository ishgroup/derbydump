package com.db.exporter.writer;


import com.db.exporter.config.Configuration;
import com.db.exporter.main.OutputThread;
import org.apache.log4j.Logger;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;

import static junit.framework.Assert.assertEquals;

public class OutputThreadTest {

	private static final Logger LOGGER = Logger.getLogger(OutputThreadTest.class);

	private static Configuration config;
	private static final String RESOURCE_DUMP_LOCATION = "./target/writer_test.out";
	private static OutputThread output;

	@BeforeClass
	public static void setUp() throws Exception {
		config = Configuration.getConfiguration();
		config.setOutputFilePath(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath());

		output = new OutputThread();
	}

	@Test
	public void testAdd() throws Exception {
		Thread writer = new Thread(output, "writer test");
		writer.start();

		output.add("Some text");
		writer.interrupt();
		writer.join();

		// Now let's read the output and see what is in it
		BufferedReader in = new BufferedReader( new FileReader(new File(RESOURCE_DUMP_LOCATION).getCanonicalPath()) );
		String line = in.readLine();
		in.close();

		assertEquals("File writer didn't write correct text.", line, "Some text");
	}
}
