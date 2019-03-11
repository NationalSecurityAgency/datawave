package datawave.webservice.query.util;

import com.sun.org.apache.xml.internal.utils.XMLChar;
import org.junit.Test;

import static org.junit.Assert.*;

public class XMLUtilTest {
    @Test
    public void testAllCharacters() {
        for (int i = 0; i < 0x300000; ++i) {
            // Uses an internal class, but only for testing.
            boolean expectedResult = XMLChar.isValid(i);
            assertEquals("Mismatch for 0x" + Integer.toHexString(i), expectedResult, XMLUtil.isValidXMLChar(i));
        }
    }
    
    @Test
    public void testValidXMLString() {
        assertTrue(XMLUtil.isValidXML("This is valid XML \u0009\r\n \u0021 \uD1FF"));
    }
    
    @Test
    public void testInvalidXMLString() {
        assertFalse(XMLUtil.isValidXML("This \u0002 is not valid"));
    }
}
