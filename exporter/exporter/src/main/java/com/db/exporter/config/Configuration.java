package com.db.exporter.config;

import com.db.exporter.writer.QueueManager;

/**
 * This class is designed and responsible for holding all the details which is
 * necessary for the configuration of the application.
 * 
 * @author Abhijeet
 * 
 */
public class Configuration {
	/**
	 * User name for the derby database
	 */
	private String userName;
	/**
	 * User password for the derby database
	 */
	private String password;
	/**
	 * driver name for derby database
	 */
	private String driverName;
	/**
	 * Path of the folder containing derby database file system
	 */
	private String derbyDbPath;
	/**
	 * Maximum size of the queue used in {@link QueueManager}.
	 */
	private int queueMaxSize;
	/**
	 * Path of the file in which application will write(or dump) data for Mysql
	 * database usage.
	 */
	private String dumpFilePath;

	/**
	 * @return the userName
	 */
	public String getUserName() {
		return userName;
	}

	/**
	 * @param userName
	 *            the userName to set
	 */
	public void setUserName(String userName) {
		this.userName = userName;
	}

	/**
	 * @return the password
	 */
	public String getPassword() {
		return password;
	}

	/**
	 * @param password
	 *            the password to set
	 */
	public void setPassword(String password) {
		this.password = password;
	}

	/**
	 * @return the derbyDbPath
	 */
	public String getDerbyDbPath() {
		return derbyDbPath;
	}

	/**
	 * @param derbyDbPath
	 *            the derbyDbPath to set
	 */
	public void setDerbyDbPath(String derbyDbPath) {
		this.derbyDbPath = derbyDbPath;
	}

	/**
	 * @return the driverName
	 */
	public String getDriverName() {
		return driverName;
	}

	/**
	 * @param driverName
	 *            the driverName to set
	 */
	public void setDriverName(String driverName) {
		this.driverName = driverName;
	}

	/**
	 * @return the queueMqxSize
	 */
	public int getQueueMaxSize() {
		return queueMaxSize;
	}

	/**
	 * @param queueMqxSize
	 *            the queueMqxSize to set
	 */
	public void setQueueMaxSize(int queueMaxSize) {
		this.queueMaxSize = queueMaxSize;
	}

	/**
	 * @return the dumpFilePath
	 */
	public String getDumpFilePath() {
		return dumpFilePath;
	}

	/**
	 * @param dumpFilePath
	 *            the dumpFilePath to set
	 */
	public void setDumpFilePath(String dumpFilePath) {
		this.dumpFilePath = dumpFilePath;
	}

}
