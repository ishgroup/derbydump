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
		String test1 = "'Single quotes'";
		assertEquals("Single quote", "\\'Single quotes\\'", Column.escapeQuotes(test1));
		
		String test2 = "''Single quotes twice''";
		assertEquals("Single quotes twice", "\\'\\'Single quotes twice\\'\\'", Column.escapeQuotes(test2));
		
		String test3 = "Tab\t";
		assertEquals("Tab", "Tab\\t", Column.escapeQuotes(test3));

		String test4 = "Single backslash\\";
		assertEquals("Backslash", "Single backslash\\\\", Column.escapeQuotes(test4));

		String test5 = "Newline\n and carriage return\r";
		assertEquals("Newline", "Newline\\n and carriage return\\r", Column.escapeQuotes(test5));

		
	}
}
