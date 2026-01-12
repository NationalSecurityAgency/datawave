package datawave.core.iterators.compress.event;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.accumulo.core.data.Key;
import org.apache.hadoop.io.Text;
import org.junit.jupiter.api.Test;

public class EventMarkerUtilTest {

    private final Key rawMarker = new Key("row", "cf", "raw\u00001-1");
    private final Key gzipKey = new Key("row", "cf", "gzip\u00001-1");
    private final Key zstdKey = new Key("row", "cf", "zstd\u00001-1");
    private final Key fiKey = new Key("row", "fi\0FIELD", "value\0datatype\0uid");

    @Test
    public void testKeyIsMarker() {
        assertTrue(EventMarkerUtil.isMarker(rawMarker));
        assertTrue(EventMarkerUtil.isMarker(gzipKey));
        assertTrue(EventMarkerUtil.isMarker(zstdKey));
        assertFalse(EventMarkerUtil.isMarker(fiKey));
    }

    @Test
    public void testTextIsMarker() {
        assertTrue(EventMarkerUtil.isMarker(new Text("raw\u00001-1")));
        assertTrue(EventMarkerUtil.isMarker(new Text("gzip\u00001-1")));
        assertTrue(EventMarkerUtil.isMarker(new Text("zstd\u00001-1")));
        assertFalse(EventMarkerUtil.isMarker(new Text("value\0datatype\0uid")));
    }

    @Test
    public void testStringIsMarker() {
        assertTrue(EventMarkerUtil.isMarker("raw\u00001-1"));
        assertTrue(EventMarkerUtil.isMarker("gzip\u00001-1"));
        assertTrue(EventMarkerUtil.isMarker("zstd\u00001-1"));
        assertFalse(EventMarkerUtil.isMarker("value\0datatype\0uid"));
    }

    @Test
    public void testGetCompressionAlgorithmFromMarker() {
        assertEquals("raw", EventMarkerUtil.getCompressionAlgorithm("raw\u00001-1"));
        assertEquals("gzip", EventMarkerUtil.getCompressionAlgorithm("gzip\u00001-1"));
        assertEquals("zstd", EventMarkerUtil.getCompressionAlgorithm("zstd\u00001-1"));
    }

    @Test
    public void testCreateMarker() {
        Key eventKey = new Key("row", "datatype\0uid", "FIELD\0value", "VIZ-A", 10L);
        Key expected = new Key("row", "datatype\0uid", "raw\u00002-128", "VIZ-A", 10L);
        Key marker = EventMarkerUtil.createMarker(eventKey, "2", 128);
        assertEquals(expected, marker);
    }

    @Test
    public void testCreateMarkerWithCompressionSpecified() {
        Key eventKey = new Key("row", "datatype\0uid", "FIELD\0value", "VIZ-A", 10L);

        Key expected = new Key("row", "datatype\0uid", "gzip\u00002-128", "VIZ-A", 10L);
        Key marker = EventMarkerUtil.createMarker(eventKey, "gzip", "2", 128);
        assertEquals(expected, marker);

        expected = new Key("row", "datatype\0uid", "zstd\u00002-128", "VIZ-A", 10L);
        marker = EventMarkerUtil.createMarker(eventKey, "zstd", "2", 128);
        assertEquals(expected, marker);
    }

    @Test
    public void testCreateMarkerWithInvalidCompressionAlgorithm() {
        Key eventKey = new Key("row", "datatype\0uid", "FIELD\0value", "VIZ-A", 10L);
        assertThrows(IllegalArgumentException.class, () -> EventMarkerUtil.createMarker(eventKey, "rar", "2", 128));
    }

}
