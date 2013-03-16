package com.db.exporter.writer;

import java.io.File;
import java.io.IOException;
import java.io.Writer;

import org.apache.log4j.Logger;

import com.db.exporter.config.Configuration;
import com.db.exporter.main.Dumper;
import com.db.exporter.utils.IOUtils;

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
		Writer streamWriter = null;
		try {
			File file = new File(m_config.getDumpFilePath());
			streamWriter = IOUtils.getOutputStream(file);
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
			System.out.println("TotalTime::"
					+ (System.currentTimeMillis() - Dumper.startTime) / 1000);
		} catch (IOException e) {
		} finally {
			try {
				if (streamWriter != null) {
					streamWriter.close();
				} else {
					LOGGER.error("Could not get File stream");
				}
			} catch (IOException e) {
				LOGGER.error("Could not close the stream writer: "
						+ e.getMessage());
			}
		}
	}
}
