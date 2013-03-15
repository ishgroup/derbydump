package com.db.exporter.reader;

/**
 * Logical module representing a reader which reads from a database and writes
 * to a buffer.
 */
public interface IDatabaseReader {	
	public void readMetaData(String schemaName);
}
