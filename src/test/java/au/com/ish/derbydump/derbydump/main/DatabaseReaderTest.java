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

import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class DatabaseReaderTest {
	@Test
	public void testProcessBinaryData() throws Exception {

	}

	@Test
	public void testProcessClobData() throws Exception {

	}


	@Test
	public void testEscapeQuotes(){
		String test1 = "Test for escaping the quotes (here's it goes)";
		String test2 = "Test for escaping the double quotes (here''s it goes)";

		assertEquals("Single quote", "Test for escaping the quotes (here''s it goes)", DatabaseReader.escapeQuotes(test1));
		assertEquals("Double quote", "Test for escaping the double quotes (here''''s it goes)", DatabaseReader.escapeQuotes(test2));
	}
}
