package com.db.exporter.main;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.db.exporter.config.Configuration;
import com.db.exporter.config.PropertyLoader;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;


public class Dumper {
	
	public static long startTime;
	private static final Logger LOGGER = Logger.getLogger(Dumper.class);
	
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
		    //c:/> java -jar -DderbyDbPath=D:/onCourse -Duser=report -Dpassword=948ty -DdriverName=org.apache.derby.jdbc.EmbeddedDriver -Dschema=onCourse -DbufferSize=20000 -DdumpPath=D:/test.sql Dumper   
			
			if(args.length != 0){
				LOGGER.debug("Usage: [-options] .jar");
				LOGGER.debug("\t-help or ?\t: Display help");
				LOGGER.debug("\t-DderbyDbPath\t: Derby database path");
				LOGGER.debug("\t-Duser\t\t: Database username, if available");
				LOGGER.debug("\t-Dpassword\t: Database password, if available");
				LOGGER.debug("\t-DdriverName\t: Database driver name to be used.\n If none is provided 'org.apache.derby.jdbc.EmbeddedDriver' will be used");
				LOGGER.debug("\t-Dschema\t: Database Schema name");
				LOGGER.debug("\t-DbufferSize\t: Internal buffer size to be used for buffering. Note: Ignore if unsure");
				LOGGER.debug("\t-DdumpPath\t: File path for the dump file");
				System.exit(0);
			}
			
			
			String userName = System.getProperty("user");
			String password = System.getProperty("password");
			String derbyDbPath = System.getProperty("derbyDbPath");
			String driverClassName = System.getProperty("driverName");
			String schemaName = System.getProperty("schema");
			LOGGER.debug(derbyDbPath);
			String max = System.getProperty("bufferSize");
			int maxBufferSize = max==null?0: Integer.valueOf(max);
			String dumpFilePath = System.getProperty("dumpPath");
			
			Properties prop = PropertyLoader.loadProperties("dump");
			userName = userName==null? prop.getProperty("db.userName"):userName;
			password = password==null? prop.getProperty("db.password"):password;
			derbyDbPath = derbyDbPath==null? prop.getProperty("db.derbyDbPath"):derbyDbPath;
			driverClassName = driverClassName==null? prop.getProperty("db.driverClassName"):driverClassName;
			schemaName = schemaName==null? prop.getProperty("db.schemaName"):schemaName;
			maxBufferSize = maxBufferSize==0? Integer.valueOf(prop.getProperty("dump.buffer.size")): maxBufferSize;
			dumpFilePath = dumpFilePath==null? prop.getProperty("dump.buffer.dumpPath"):dumpFilePath;
			
			Configuration config = Configuration.getConfiguration(userName, password, derbyDbPath, driverClassName, schemaName, maxBufferSize, dumpFilePath);
			
			LOGGER.debug("Configuration:");
			LOGGER.debug("\tuser \t\t=" + userName);
			LOGGER.debug("\tpassword \t=" + password);
			LOGGER.debug("\tderbyDbPath \t=" + derbyDbPath);
			LOGGER.debug("\tdriverName \t=" + driverClassName);
			LOGGER.debug("\tschema \t\t=" + schemaName);
			LOGGER.debug("\tbufferSize \t=" + maxBufferSize);
			LOGGER.debug("\tdumpPath \t=" + dumpFilePath);
			
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
