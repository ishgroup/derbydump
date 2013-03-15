package com.db.exporter.writer;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.apache.log4j.Logger;

import com.db.exporter.config.Configuration;
import com.db.exporter.main.Dumper;
import com.db.exporter.utils.IOUtils;

/**
 * This class encapsulates a thread which is responsible for picking data
 * element from the queue( refer {@link BufferManager} and write to the file
 * which will eventually used by a mysqldump application for reloading data into
 * mysql database.
 * 
 */
public class FileWriter implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(FileWriter.class);
	private IBuffer m_buffer;
	private Configuration configuration;

	public FileWriter() {
		m_buffer = BufferManager.getBufferInstance();
		configuration = Configuration.getConfiguration();
	}

	/**
	 * Main body of the thread. Here, data element is getting removed from queue
	 * and eventually written on the file
	 */
	public void run() {
		Writer streamWriter = null;
		try {
			File file = new File(configuration.getDumpFilePath());
			streamWriter =  IOUtils.getOutputStream(file);
			synchronized (BufferManager.BUFFER_TOKEN) {
				while (!BufferManager.isReadingComplete()) {
						if (!Thread.interrupted()) {
							try {
								BufferManager.BUFFER_TOKEN.wait();
							} catch (InterruptedException e) {
								LOGGER.info("Reader interrupted. Killing writer.");
								return;
							}
							// writing the data to the file
						if (m_buffer.size() == 0) {
							break;
						}
							IOUtils.write(streamWriter, m_buffer);
							streamWriter.flush();
							BufferManager.BUFFER_TOKEN.notify();
						}
					}
				}
			System.out.println("TotalTime::"+(System.currentTimeMillis() - Dumper.startTime)/1000);
		} catch (IOException e) {
		} finally{
			try {
				streamWriter.close();
			} catch (IOException e) {
				LOGGER.error(e.getMessage());
			}
		}
	}
}
