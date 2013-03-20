package com.db.exporter.writer;

import com.db.exporter.config.Configuration;
import com.db.exporter.main.DerbyDump;
import org.apache.log4j.Logger;

import java.io.*;

/**
 * Logical module representing a writer/consumer which flushes the buffer and
 * writes to a stream.
 */
public class FileWriter implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(FileWriter.class);
	private IBuffer m_buffer;
	private Configuration m_config;

	public FileWriter(Configuration config, IBuffer buffer) {
		m_buffer = buffer;
		m_config = config;
	}

	/**
	 * Writing logic.
	 * 
	 * Until Reader is completely done, keep flushing and writing the buffer to
	 * a specified file.
	 * 
	 */
	public void run() {
		LOGGER.debug("File writer intializing...");
		Writer streamWriter = null;
		try {
			File file = new File(m_config.getOutputFilePath());

			if (!file.exists()) {
				file.createNewFile();
			}
			streamWriter = new OutputStreamWriter(new FileOutputStream(file), "UTF-8");

			synchronized (BufferManager.BUFFER_TOKEN) {
				while (!BufferManager.isReadingComplete()) {
					if (!Thread.interrupted()) {
						try {
							BufferManager.BUFFER_TOKEN.wait();
						} catch (InterruptedException e) {
							LOGGER.info("Writer interrupted. Exiting now.. ");
							return;
						}
						// writing the data to the file
						if (m_buffer.size() == 0) {
							break;
						}
						streamWriter.append(m_buffer.flush());
						streamWriter.flush();
						BufferManager.BUFFER_TOKEN.notify();
					}
				}
			}
			LOGGER.debug("TotalTime:"
					+ (System.currentTimeMillis() - DerbyDump.startTime) / 1000);
		} catch (IOException e) {
		} finally {
			try {
				if (streamWriter != null) {
					streamWriter.close();
				} else {
					LOGGER.error("File stream null");
				}
			} catch (IOException e) {
				LOGGER.error("Could not close the stream writer: "+ e.getMessage());
			}
		}
		LOGGER.debug("Writing done.");
		LOGGER.debug("Dump completed!");
	}
}
