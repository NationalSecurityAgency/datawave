package nsa.datawave.data.normalizer;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

public class HexStringNormalizerTest {
    
    private final HexStringNormalizer normalizer = new HexStringNormalizer();
    
    @Test
    public void testAllHexCharacters() {
        assertEquals("Test all hex characters", "1234567890abcdefabcdef", normalizer.normalize("1234567890abcdefABCDEF"));
        assertEquals("Test all hex characters w/0x", "1234567890abcdefabcdef", normalizer.normalize("0x1234567890abcdefABCDEF"));
    }
    
    @Test
    public void testOddLenghtValidHexString() {
        assertEquals("Test odd length", "0123", normalizer.normalize("123"));
        assertEquals("Test odd length w/0x", "0123", normalizer.normalize("0x123"));
        assertEquals("Test odd length", "0abcde", normalizer.normalize("abCde"));
        assertEquals("Test odd length w/0x", "0abcde", normalizer.normalize("0xabCde"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHexStringEmpty() {
        normalizer.normalize("");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHexStringPrefixOnly() {
        assertEquals("Test invalid hex string w/0x", "0x", normalizer.normalize("0x"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHexStringNotHex() {
        assertEquals("Test invalid hex string", "Not Hex", normalizer.normalize("Not Hex"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testInvalidHexStringWithG() {
        assertEquals("Test invalid hex string", "aBcDeFg12345", normalizer.normalize("aBcDeFg12345"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConvertFieldRegexEmpty() {
        normalizer.normalizeRegex("");
    }
    
    @Test
    public void testConvertFieldRegexToLower() {
        assertEquals("Test convertFieldRegex", "1234567890abcdefabcdef", normalizer.normalizeRegex("1234567890abcdefABCDEF"));
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConvertFieldRegexNull() {
        normalizer.normalizeRegex(null);
    }
}
