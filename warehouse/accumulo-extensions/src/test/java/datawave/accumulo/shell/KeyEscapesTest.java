package datawave.accumulo.shell;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.nio.charset.StandardCharsets;

import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class KeyEscapesTest {

    @Test
    public void testNullEscapeBecomesANullByte() {
        assertEquals(new Text("datatype\0uid".getBytes(StandardCharsets.UTF_8)), KeyEscapes.decode("datatype\\0uid"));
    }

    @Test
    public void testHexEscapes() {
        assertEquals(new Text(new byte[] {0x00, (byte) 0xff, 0x41}), KeyEscapes.decode("\\x00\\xFF\\x41"));
    }

    @Test
    public void testWhitespaceAndBackslashEscapes() {
        assertEquals(new Text("a\nb\rc\td\\e"), KeyEscapes.decode("a\\nb\\rc\\td\\\\e"));
    }

    @Test
    public void testValueWithoutEscapesIsUnchanged() {
        assertEquals(new Text("20260818_0"), KeyEscapes.decode("20260818_0"));
    }

    @Test
    public void testMultiByteCharactersSurviveDecoding() {
        assertEquals(new Text("café\0uid".getBytes(StandardCharsets.UTF_8)), KeyEscapes.decode("café\\0uid"));
    }

    @Test
    public void testDecodingCanBeDisabled() {
        assertEquals(new Text("datatype\\0uid"), KeyEscapes.decode("datatype\\0uid", false));
    }

    @Test
    public void testUnrecognizedEscapeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> KeyEscapes.decode("datatype\\quid"));
    }

    @Test
    public void testDanglingBackslashIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> KeyEscapes.decode("datatype\\"));
    }

    @Test
    public void testTruncatedHexEscapeIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> KeyEscapes.decode("\\x0"));
    }

    @Test
    public void testInvalidHexDigitIsRejected() {
        assertThrows(IllegalArgumentException.class, () -> KeyEscapes.decode("\\xzz"));
    }

    @Test
    public void testEncodeRoundTripsThroughDecode() {
        Text raw = new Text("fi\0FIELD\0value\0datatype\0uid".getBytes(StandardCharsets.UTF_8));
        assertEquals("fi\\0FIELD\\0value\\0datatype\\0uid", KeyEscapes.encode(raw));
        assertEquals(raw, KeyEscapes.decode(KeyEscapes.encode(raw)));
    }

    @Test
    public void testEncodeRendersNonPrintableBytesAsHex() {
        assertEquals("a\\x01\\xffb", KeyEscapes.encode(new Text(new byte[] {'a', 0x01, (byte) 0xff, 'b'})));
    }
}
