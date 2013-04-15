/*
 * Copyright 2013 ish group pty ltd
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.com.ish.derbydump.derbydump.main;

import au.com.ish.derbydump.derbydump.config.Configuration;
import org.apache.log4j.Logger;

import java.io.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * Logical module representing a writer/consumer which flushes the buffer and
 * writes to a stream.
 */
public class OutputThread implements Runnable {
	private static final Logger LOGGER = Logger.getLogger(OutputThread.class);

	private Writer out;
	private final BlockingQueue<String> queue = new LinkedBlockingQueue<String>(1024);
	private boolean stopping = false;

	public OutputThread() {
		Configuration config = Configuration.getConfiguration();

		try {
			File file = new File(config.getOutputFilePath());
			if (file.exists()) {
				file.delete();
			}

			file.createNewFile();

			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(file),"UTF-8"), config.getBufferMaxSize() * 1024);

		} catch (IOException e) {
			LOGGER.error("Could not write to the file " + config.getOutputFilePath());
		}
	}

	public void add(String data) {
		try {
			if (data != null) {
				queue.put(data);
			}
		} catch (InterruptedException ignored) {}
	}

	/**
	 * Writing logic.
	 * 
	 * Until Reader is completely done, keep flushing and writing the buffer to
	 * a specified file.
	 * 
	 */
	public void run() {
		long startTime = System.currentTimeMillis();

		LOGGER.debug("File writer intializing...");

		try {
			while (!stopping || queue.size() > 0) {
				try {
					String item = queue.take();
					if (LOGGER.isDebugEnabled()) {
						LOGGER.debug("Item taken from queue: " + item);
					}
					if (item != null) {
						out.write(item);
					}
				} catch (InterruptedException e) {
					LOGGER.debug("Writer has been asked to stop.");
					stopping = true;
				}
			}

			out.flush();
			out.close();
		} catch (IOException e) {
			LOGGER.error("Could not close the stream writer: " + e.getMessage());
		}

		LOGGER.debug("Total time (seconds): " + (System.currentTimeMillis() - startTime) / 1000);
		LOGGER.debug("Writing done.");
	}
}
