package au.com.ish.derbydump.derbydump.metadata;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class ColumnTest {

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

		assertEquals("Single quote", "Test for escaping the quotes (here''s it goes)", Column.escapeQuotes(test1));
		assertEquals("Double quote", "Test for escaping the double quotes (here''''s it goes)", Column.escapeQuotes(test2));
	}
}
