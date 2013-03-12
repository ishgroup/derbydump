package com.db.exporter.writer;

import java.util.concurrent.ArrayBlockingQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import com.db.exporter.config.Configuration;

/**
 * This class is responsible for the management of data and queue {@link ArrayBlockingQueue}. It has
 * methods for adding and removing elements from the queue.
 * 
 * @author Abhijeet
 * 
 */
public class QueueManager {

	private static Log logger = LogFactory.getLog(QueueManager.class);
	private Configuration configuration;
	private ArrayBlockingQueue<String> dataQueue;
	private boolean isReadingComplete = false;

	/**
	 * Default constructor
	 */
	public QueueManager() {

	}

	/**
	 * This method is responsible for adding data to the queue.
	 * @param data
	 */
	public void addDataToQueue(String data) {
		try {
			dataQueue.put(data);
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
	}

	/**
	 * This method is responsible for removing data from the queue.
	 * @return
	 */
	public String removDataFromQueue() {
		String data = null;
		try {
			data = dataQueue.take();
		} catch (InterruptedException e) {
			logger.error(e.getMessage(), e);
		}
		return data;
	}

	/**
	 * This method is responsible for returning size of the queue.
	 * @return
	 */
	public int getSizeOfDataQueue() {
		return dataQueue.size();
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
		dataQueue = new ArrayBlockingQueue<String>(
				configuration.getQueueMaxSize());
	}

	/**
	 * @return the isReadingComplete
	 */
	public boolean isReadingComplete() {
		return isReadingComplete;
	}

	/**
	 * @param isReadingComplete the isReadingComplete to set
	 */
	public void setReadingComplete(boolean isReadingComplete) {
		this.isReadingComplete = isReadingComplete;
	}

}
