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
