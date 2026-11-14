package datawave.data.normalizer;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.util.Locale;

import org.junit.jupiter.api.Test;

public class LcNoDiacriticsNormalizerTest {
    @Test
    public void test1() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        String b = null;
        String n1 = norm.normalize(b);

        assertNull(n1);

    }

    @Test
    public void testNullRegex() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertNull(norm.normalizeRegex(null));
    }

    @Test
    public void testEmpty() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertEquals("", norm.normalize(""));
        assertEquals("", norm.normalizeRegex(""));
    }

    @Test
    public void testAsciiValues() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertEquals("customer.name-123@example.org", norm.normalize("CUSTOMER.Name-123@Example.ORG"));
        assertEquals("already lowercase", norm.normalize("already lowercase"));

        StringBuilder ascii = new StringBuilder();
        for (char c = 0; c <= 0x7f; c++) {
            ascii.append(c);
        }
        String value = ascii.toString();
        assertEquals(value.toLowerCase(Locale.ENGLISH), norm.normalize(value));
    }

    @Test
    public void testUnicodeValues() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertEquals("cafe", norm.normalize("CAF\u00c9"));
        assertEquals("cafe", norm.normalize("CAFE\u0301"));
        assertEquals("ascii prefix a", norm.normalize("ASCII PREFIX \u00c0"));
        assertEquals("\uac00", norm.normalize("\u1100\u1161"));
        assertEquals("a\u1ab0", norm.normalize("A\u1ab0"));
        assertEquals("hello \ud83d\ude00", norm.normalize("HELLO \ud83d\ude00"));
    }

    @Test
    public void testAsciiRegexPreservesSyntax() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertEquals("abc.*", norm.normalizeRegex("ABC.*"));
        assertEquals("\\D+[a-z]\\S\\W", norm.normalizeRegex("\\D+[A-Z]\\S\\W"));
    }

    @Test
    public void testUnicodeRegex() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertEquals("cafe\\D+", norm.normalizeRegex("CAF\u00c9\\D+"));
        assertEquals("cafe\\D+", norm.normalizeRegex("CAFE\u0301\\D+"));
        assertEquals("\uac00.*", norm.normalizeRegex("\u1100\u1161.*"));
    }

    @Test
    public void testInvalidAsciiRegex() {
        LcNoDiacriticsNormalizer norm = new LcNoDiacriticsNormalizer();
        assertThrows(IllegalArgumentException.class, () -> norm.normalizeRegex("["));
    }
}
