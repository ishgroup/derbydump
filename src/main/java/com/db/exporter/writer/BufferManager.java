package com.db.exporter.writer;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;

import com.db.exporter.config.Configuration;

/**
 * This class is responsible for the management of data and queue
 * {@link ArrayBlockingQueue}. It has methods for adding and removing elements
 * from the queue.
 * 
 */
public class BufferManager {

	public static final Object BUFFER_TOKEN = new Object();
	private static volatile boolean g_isReadingComplete = false;
	private static IBuffer m_buffer;


	private BufferManager() {
	}

	public static synchronized IBuffer getBufferInstance() {
		if (m_buffer == null) {
			m_buffer = new StringBuilderAsBuffer();
		}
		return m_buffer;
	}

	/**
	 * @return the isReadingComplete
	 */
	public static boolean isReadingComplete() {
		return g_isReadingComplete;
	}

	/**
	 * @param isReadingComplete
	 *            the isReadingComplete to set
	 */
	public static void setReadingComplete(boolean isReadingComplete) {
		g_isReadingComplete = isReadingComplete;
	}
}

class StringBuilderAsBuffer implements IBuffer {

	StringBuilder m_buffer;

	StringBuilderAsBuffer() {
		init();
	}

	private void init(){
		m_buffer = new StringBuilder(Configuration.getConfiguration()
				.maxBufferSize());
	}
	
	public boolean isFull() {
		return m_buffer.length() >= Configuration.getConfiguration()
				.maxBufferSize();
	}

	public boolean isEmpty() {
		return m_buffer.length() <= 0;
	}

	public int size() {
		return m_buffer.length();
	}

	public int maxSize() {
		return Configuration.getConfiguration().maxBufferSize();
	}

	public String flush() throws IOException {
		 String content = m_buffer.toString();
		 init();
		 return content;
	}

	public IBuffer add(String data) {
		m_buffer.append(data);
		return this;
	}
}
