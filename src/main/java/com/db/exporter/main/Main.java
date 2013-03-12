package com.db.exporter.main;

import com.db.exporter.config.Configuration;
import com.db.exporter.reader.DerbyDBReader;
import com.db.exporter.reader.impl.DerbyDBReaderImpl;
import com.db.exporter.utils.AppContext;
import com.db.exporter.utils.DBConnectionManager;
import com.db.exporter.writer.ReaderThread;

public class Main {

	AppContext appContext;
	Configuration configuration;
	DerbyDBReader derbyDBReader;
	ReaderThread readerThread;
	DBConnectionManager dbConnectionManager;

	public Main() {
		appContext = new AppContext();
		setConfiguration(configuration);
		derbyDBReader = (DerbyDBReaderImpl) appContext.getContext().getBean(
				"derbyDBReaderImpl");
		dbConnectionManager = (DBConnectionManager) appContext.getContext()
				.getBean("dbConnectionManager");
	}

	public static void main(String[] args) {
		try {
			Main main = new Main();
			Long startTime = System.currentTimeMillis();
			main.startReader();
			main.close();
			Long endTime = System.currentTimeMillis();
			Long totalTimeinSecs = (endTime - startTime)/1000;
			System.out.println("TotalTime::"+totalTimeinSecs);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public void startReader() {
		derbyDBReader.readMetaData();
	}

	public void close() {
		dbConnectionManager.disconnect();
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
		this.configuration = (Configuration) appContext.getContext().getBean(
				"configuration");
		;
	}

}
