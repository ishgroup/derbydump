package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.OutputThread;
import org.apache.log4j.Logger;


public class DerbyDump {
	
	private static final Logger LOGGER = Logger.getLogger(DerbyDump.class);
	
	public static void main(String[] args) {

			Configuration config = Configuration.getConfiguration();

			LOGGER.debug("Configuration:");
			LOGGER.debug("\tuser =" + config.getUserName());
			LOGGER.debug("\tpassword =" + config.getPassword());
			LOGGER.debug("\tderbyDbPath =" + config.getDerbyDbPath());
			LOGGER.debug("\tdriverName =" + config.getDriverClassName());
			LOGGER.debug("\tschema =" + config.getSchemaName());
			LOGGER.debug("\tbuffer size =" + config.getBufferMaxSize());
			LOGGER.debug("\toutput file path =" + config.getOutputFilePath());

			OutputThread output = new OutputThread();

			Thread reader = new Thread(new DatabaseReader(output), "Database_reader");
			Thread writer = new Thread(output, "File_Writer");
			
			reader.start();
			writer.start();

		try {
			// Now let's wait for the reader to finish
			reader.join();

			// And let the writer know that no more data is coming
			writer.interrupt();
			writer.join();

		} catch (InterruptedException ignored) {}

	}	
}
