package com.db.exporter.writer;

import java.io.IOException;

import com.db.exporter.config.Configuration;

/**
 * Singleton: Encapsulates the buffer used for reading and writing.
 * 
 */
public class BufferManager {

	/*Token for buffer access*/
	protected static final Object BUFFER_TOKEN = new Object();
	private static volatile boolean g_isReadingComplete = false;
	private static IBuffer m_buffer;


	private BufferManager() {
	}

	/**
	 * @return Instance of an {@link IBuffer}
	 */
	public static synchronized IBuffer getBufferInstance() {
		if (m_buffer == null) {
			m_buffer = new StringBuilderAsBuffer();
		}
		return m_buffer;
	}

	/**
	 * @return Returns reader thread's status.
	 */
	public static boolean isReadingComplete() {
		return g_isReadingComplete;
	}

	/**
	 * @param isReadingComplete
	 *            the isReadingComplete to set
	 */
	protected static void setReadingComplete(boolean isReadingComplete) {
		g_isReadingComplete = isReadingComplete;
	}
}

/**
 * IBuffer using StringBuilder as the underlying buffer implementation.
 */
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
