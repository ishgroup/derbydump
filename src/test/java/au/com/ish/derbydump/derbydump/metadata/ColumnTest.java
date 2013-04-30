package au.com.ish.derbydump.derbydump.metadata;

import java.io.InputStream;
import java.sql.Clob;

import javax.sql.rowset.serial.SerialBlob;
import javax.sql.rowset.serial.SerialClob;

import org.apache.commons.io.IOUtils;
import org.junit.Test;

import static junit.framework.Assert.assertEquals;


public class ColumnTest {

	@Test
	public void testProcessBinaryData() throws Exception {
		InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream("Penguins.jpg");
		byte[] inputData = IOUtils.toByteArray(is);

		assertEquals(5569, inputData.length);

		String result = Column.processBinaryData(new SerialBlob(inputData));

		assertEquals(11140, result.length());

		assertEquals("0x61", Column.processBinaryData(new SerialBlob(new byte[]{'a'})));
		assertEquals("0x0A", Column.processBinaryData(new SerialBlob(new byte[]{'\n'})));
	}

	@Test
	public void testProcessNullBinaryData() throws Exception {
		assertEquals("NULL", Column.processBinaryData(null));
		assertEquals("NULL", Column.processBinaryData(new SerialBlob(new byte[]{})));
	}

	@Test
	public void testProcessClobData() throws Exception {
		String oneSimpleClob = "one simple clob";
		Clob inputClob = new SerialClob(oneSimpleClob.toCharArray());

		String processedString = Column.processClobData(inputClob);

		assertEquals("'"+oneSimpleClob+"'", processedString);
	}

	@Test
	public void testProcessNullClobData() throws Exception {
		assertEquals("NULL", Column.processClobData(null));
		assertEquals("''", Column.processClobData(new SerialClob("".toCharArray())));
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
