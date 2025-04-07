package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;

public class HexStringNormalizerTest {
    
    private final HexStringNormalizer normalizer = new HexStringNormalizer();
    
    @Test
    public void testAllHexCharacters() {
        assertEquals("1234567890abcdefabcdef", normalizer.normalize("1234567890abcdefABCDEF"), "Test all hex characters");
        assertEquals("1234567890abcdefabcdef", normalizer.normalize("0x1234567890abcdefABCDEF"), "Test all hex characters w/0x");
    }
    
    @Test
    public void testOddLenghtValidHexString() {
        assertEquals("0123", normalizer.normalize("123"), "Test odd length");
        assertEquals("0123", normalizer.normalize("0x123"), "Test odd length w/0x");
        assertEquals("0abcde", normalizer.normalize("abCde"), "Test odd length");
        assertEquals("0abcde", normalizer.normalize("0xabCde"), "Test odd length w/0x");
    }
    
    @Test
    public void testInvalidHexStringEmpty() {
        assertThrows(IllegalArgumentException.class, () -> normalizer.normalize(""));
    }
    
    @Test
    public void testInvalidHexStringPrefixOnly() {
        assertThrows(IllegalArgumentException.class, () -> normalizer.normalize("0x"), "Test invalid hex string w/0x");
    }
    
    @Test
    public void testInvalidHexStringNotHex() {
        assertThrows(IllegalArgumentException.class, () -> normalizer.normalize("Not Hex"), "Test invalid hex string");
    }
    
    @Test
    public void testInvalidHexStringWithG() {
        assertThrows(IllegalArgumentException.class, () -> normalizer.normalize("aBcDeFg12345"), "Test invalid hex string");
    }
    
    @Test
    public void testConvertFieldRegexEmpty() {
        assertThrows(IllegalArgumentException.class, () -> normalizer.normalizeRegex(""));
    }
    
    @Test
    public void testConvertFieldRegexToLower() {
        assertEquals("1234567890abcdefabcdef", normalizer.normalizeRegex("1234567890abcdefABCDEF"), "Test convertFieldRegex");
    }
    
    @Test
    public void testConvertFieldRegexNull() {
        assertThrows(IllegalArgumentException.class, () -> normalizer.normalizeRegex(null));
    }
}
