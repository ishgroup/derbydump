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

package au.com.ish.derbydump.derbydump;

import au.com.ish.derbydump.derbydump.main.DerbyDumpTest;
import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Keeps a suite of classes to be running during the test execution Write full
 * class name to avoid a large count of imports Keep an alphabetic order
 * 
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ DerbyDumpTest.class })
public class AllTests {
	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void runBeforeClass() throws Exception {
	}
}
