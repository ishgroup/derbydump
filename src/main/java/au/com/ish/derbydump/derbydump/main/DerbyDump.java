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


public class DerbyDump {
	
	private static final Logger LOGGER = Logger.getLogger(DerbyDump.class);
	
	public static void main(String[] args) {

			Configuration config = Configuration.getConfiguration();

			LOGGER.debug("Configuration:");
			LOGGER.debug("\tuser =" + config.getUserName());
			LOGGER.debug("\tpassword =" + config.getPassword());
			LOGGER.debug("\tderbyDbPath =" + config.getDerbyDbPath());
			LOGGER.debug("\tdriverName =" + config.getDriverClassName());
			LOGGER.debug("\tschema =" + config.getSchemaName());
			LOGGER.debug("\tbuffer size =" + config.getBufferMaxSize());
			LOGGER.debug("\toutput file path =" + config.getOutputFilePath());

			OutputThread output = new OutputThread();
			Thread writer = new Thread(output, "File_Writer");
			writer.start();

			new DatabaseReader(output);
		try {
			// Let the writer know that no more data is coming
			writer.interrupt();
			writer.join();

		} catch (InterruptedException ignored) {}

	}	
}
