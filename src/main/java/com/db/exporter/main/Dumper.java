package com.db.exporter.main;

import com.db.exporter.reader.impl.DatabaseReader;
import com.db.exporter.writer.FileWriter;


public class Dumper {
	
	public static long startTime;
	
	public static void main(String[] args) {
		try {
			startTime = System.currentTimeMillis();
	
			Thread reader = new Thread(new DatabaseReader(), "Database_reader");
			Thread writer = new Thread(new FileWriter(), "File_Writer");
			
			reader.start();
			writer.start();
			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}	
}
