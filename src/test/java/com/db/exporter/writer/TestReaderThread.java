package com.db.exporter.writer;

import org.springframework.context.ApplicationContext;

import com.db.exporter.util.AppContext;


import junit.framework.TestCase;

public class TestReaderThread extends TestCase {
	
	private ReaderThread readerThread;
	
	@Override
	protected void setUp() throws Exception{
		ApplicationContext context = AppContext.getContext();
		readerThread = (ReaderThread) context.getBean("readerThread");
	}

	public void testInit() {
		boolean expected = true;
		boolean actual = readerThread.isRunning();
		assertEquals(expected, actual);
	}

	

	public void testStopReader() {
		boolean expected = false;
		readerThread.stopReader();
		boolean actual = readerThread.isRunning();
		assertEquals(expected, actual);
	}

}
