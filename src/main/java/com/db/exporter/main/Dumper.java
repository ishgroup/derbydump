package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.writer.BufferManager;
import com.db.exporter.writer.DatabaseReader;
import com.db.exporter.writer.FileWriter;


public class Dumper {
	
	public static long startTime;
	
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
			
			Thread reader = new Thread(new DatabaseReader(Configuration.getConfiguration(), BufferManager.getBufferInstance()), "Database_reader");
			Thread writer = new Thread(new FileWriter(Configuration.getConfiguration(), BufferManager.getBufferInstance()), "File_Writer");
			
			reader.start();
			writer.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
