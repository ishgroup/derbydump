package com.db.exporter.writer;

import java.io.IOException;

/**
 * Represents the in memory buffer used by Reader/Writer threads. Can have
 * significant impact on performance.
 * 
 */
public interface IBuffer {

	/**
	 * @return Is buffer full?
	 */
	public boolean isFull();

	/**
	 * @return Is buffer empty?
	 */
	public boolean isEmpty();

	/**
	 * @return Current size of the buffer
	 */
	public int size();

	/**
	 * @return Buffer capacity
	 */
	public int maxSize();

	/**
	 * Flushes the buffer.
	 * 
	 * @return String representation of the flushed contents.
	 */
	public String flush() throws IOException;

	/**
	 * Add data to the buffer.
	 * 
	 * @param data
	 * @return IBuffer (to support chaining)
	 */
	public IBuffer add(String data);
}
