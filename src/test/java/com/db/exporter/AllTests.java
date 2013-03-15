package com.db.exporter;

import org.junit.BeforeClass;
import org.junit.runner.RunWith;
import org.junit.runners.Suite;

/**
 * Keeps a suite of classes to be running during the test execution Write full
 * class name to avoid a large count of imports Keep an alphabetic order
 * 
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({ com.db.exporter.main.DumpTest.class })
public class AllTests {
	/**
	 * @throws Exception
	 */
	@BeforeClass
	public static void runBeforeClass() throws Exception {
	}
}
