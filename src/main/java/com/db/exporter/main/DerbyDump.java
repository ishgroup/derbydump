package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;
import org.apache.log4j.Logger;


public class DerbyDump {
	
	public static long startTime;
	private static final Logger LOGGER = Logger.getLogger(DerbyDump.class);
	
	public static void main(String[] args) {
		try {
			startTime = System.currentTimeMillis();
			/*
			 * Usage instructions:
			 * 
			 * Step 1 :Set up the dump.properties.
			 * Step 2 :Start a reader thread.
			 * Step 3 :Start a writer thread.
			 * 
			 * After dump has been created the threads will kill themselves.
			 */

			Configuration config = Configuration.getConfiguration();

			LOGGER.debug("Configuration:");
			LOGGER.debug("\tuser =" + config.getUserName());
			LOGGER.debug("\tpassword =" + config.getPassword());
			LOGGER.debug("\tderbyDbPath =" + config.getDerbyDbPath());
			LOGGER.debug("\tdriverName =" + config.getDriverClassName());
			LOGGER.debug("\tschema =" + config.getSchemaName());
			LOGGER.debug("\tbuffer size =" + config.getBufferMaxSize());
			LOGGER.debug("\toutput file path =" + config.getOutputFilePath());
			
			Thread reader = new Thread(new DatabaseReader(config, BufferManager.getBufferInstance()), "Database_reader");
			Thread writer = new Thread(new FileWriter(config, BufferManager.getBufferInstance()), "File_Writer");
			
			reader.start();
			writer.start();
			
		} catch (Exception e) {
			LOGGER.debug("Failed execution: "+ e.getMessage());
			LOGGER.debug("Use -help for usage or check readMe for default configuration");
			
		}
	}	
}
