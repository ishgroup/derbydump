package com.db.exporter.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;

import com.db.exporter.writer.IBuffer;


/**
 * This is a utility class which provides a method for writing content on the file. This method
 * will add data to the already present data in the file.
 *
 */
public class IOUtils {
	
	public static void write(Writer writer, IBuffer buffer) throws IOException{
		writer.append(buffer.flush());
	}

	public static Writer getOutputStream(File file) throws IOException {
		if(!file.exists()){
            file.createNewFile();
		}
		return new FileWriter(file);
	}
	
}
