package com.db.exporter.writer;

import java.io.IOException;

public interface IBuffer {
	
	public boolean isFull();

	public boolean isEmpty();

	public int size();

	public int maxSize();

	public String flush() throws IOException;

	public IBuffer add(String data);
}
