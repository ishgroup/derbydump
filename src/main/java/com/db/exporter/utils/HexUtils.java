package com.db.exporter.utils;

import org.apache.log4j.Logger;

/**
 * This is an utility class which has a methods for converting binary data to
 * the hex String. 
 *
 */
public class HexUtils {

	static private final int BASELENGTH = 255;
	static private final int LOOKUPLENGTH = 16;
	static private byte[] hexNumberTable = new byte[BASELENGTH];
	static private byte[] lookUpHexAlphabet = new byte[LOOKUPLENGTH];
	static Logger LOGGER = Logger.getLogger(HexUtils.class);
	
	static {
	      for (int i = 0; i<BASELENGTH; i++ ) {
	          hexNumberTable[i] = -1;
	      }
	      for ( int i = '9'; i >= '0'; i--) {
	          hexNumberTable[i] = (byte) (i-'0');
	      }
	      for ( int i = 'F'; i>= 'A'; i--) {
	          hexNumberTable[i] = (byte) ( i-'A' + 10 );
	      }
	      for ( int i = 'f'; i>= 'a'; i--) {
	         hexNumberTable[i] = (byte) ( i-'a' + 10 );
	      }

	      for(int i = 0; i<10; i++ )
	          lookUpHexAlphabet[i] = (byte) ('0'+i );
	      for(int i = 10; i<=15; i++ )
	          lookUpHexAlphabet[i] = (byte) ('A'+i -10);
	  }
	
	/**
	   * Converts bytes to a hex string
	   */
	  static public String bytesToString(byte[] binaryData)
	  {
	      if (binaryData == null)
	          return null;
	      return new String(encode(binaryData));
	  }
	  
	  /**
	   * array of byte to encode
	   *
	   * @param binaryData
	   * @return return encode binary array
	   */
	  static public byte[] encode(byte[] binaryData) {
	      if (binaryData == null)
	          return null;
	      int lengthData   = binaryData.length;
	      int lengthEncode = lengthData * 2;
	      byte[] encodedData = new byte[lengthEncode];
	      for( int i = 0; i<lengthData; i++ ){
	          encodedData[i*2] = lookUpHexAlphabet[(binaryData[i] >> 4) & 0xf];
	          encodedData[i*2+1] = lookUpHexAlphabet[ binaryData[i] & 0xf];
	      }
	      return encodedData;
	  }
}
