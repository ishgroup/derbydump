package com.db.exporter.utils;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * This is a utility class which provides a method for writing content on the file. This method
 * will add data to the already present data in the file.
 * @author Abhijeet
 *
 */
public class IOUtils {
	static Log logger = LogFactory.getLog(IOUtils.class);

	/**
	 * This method is responsible for writing string data on the file.
	 * @param file
	 * @param data
	 */
	public static void writeToFile(File file, String data){		
	    try{
	    if(!file.exists()){
	            logger.info("File at the location "+file.getAbsolutePath() + "is not present.");
	            logger.info("Anew file will be created..");
	            file.createNewFile();
	    }
	    FileWriter out = new FileWriter(file, true);
	    BufferedWriter bf = new BufferedWriter(out);
	    bf.append(data);
	    //bf.write("foobar");
	    bf.close();
	    //PrintWriter out = new PrintWriter(new Buffer ileWriter(file,true));
	    //out.append(data);
	    out.close();
	    }catch(IOException e){
	        logger.error(e.getMessage(), e);
	    }
	}
	
}
