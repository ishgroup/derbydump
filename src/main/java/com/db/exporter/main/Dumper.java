package com.db.exporter.main;

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
			
			Thread reader = new Thread(new DatabaseReader(), "Database_reader");
			Thread writer = new Thread(new FileWriter(), "File_Writer");
			
			reader.start();
			writer.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
