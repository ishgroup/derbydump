package com.db.exporter.writer;

import java.io.File;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.db.exporter.config.Configuration;
import com.db.exporter.utils.IOUtils;

/**
 * This class encapsulates a thread which is responsible for picking data
 * element from the queue( refer {@link QueueManager} and write to the file
 * which will eventually used by a mysqldump application for reloading data into
 * mysql database.
 * 
 * @author Abhijeet
 * 
 */
public class ReaderThread implements Runnable {
	private static Log logger = LogFactory.getLog(ReaderThread.class);
	private QueueManager queueManager;
	private boolean statusofThread = true;
	private Configuration configuration;

	public ReaderThread() {
		logger.info("INITIALIZING READER THREAD");
	}

	/**
	 * Main body of the thread. Here, data element is getting removed from queue
	 * and eventually written on the file
	 */
	public void run() {
		while (statusofThread) {
			// picking the data
			String data = queueManager.removDataFromQueue();
			// writing the data to the file
			IOUtils.writeToFile(new File(configuration.getDumpFilePath()), data);
			if (queueManager.isReadingComplete()
					&& queueManager.getSizeOfDataQueue() == 0)
				statusofThread = false;
		}
	}

	/**
	 * Initialize the thread
	 */
	public void init() {
		Thread thread = new Thread(this);
		thread.start();
	}

	public boolean isRunning() {
		return statusofThread;
	}

	// stops the reader Thread
	public void stopReader() {
		if (queueManager.isReadingComplete()
				&& queueManager.getSizeOfDataQueue() == 0)
			statusofThread = false;
	}

	/**
	 * @return the queueManager
	 */
	public QueueManager getQueueManager() {
		return queueManager;
	}

	/**
	 * @param queueManager
	 *            the queueManager to set
	 */
	public void setQueueManager(QueueManager queueManager) {
		this.queueManager = queueManager;
	}

	/**
	 * @return the configuration
	 */
	public Configuration getConfiguration() {
		return configuration;
	}

	/**
	 * @param configuration
	 *            the configuration to set
	 */
	public void setConfiguration(Configuration configuration) {
		this.configuration = configuration;
	}

}
