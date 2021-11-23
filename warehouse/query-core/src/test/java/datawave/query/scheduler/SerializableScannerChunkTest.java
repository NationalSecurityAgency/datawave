package datawave.query.scheduler;

import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.Test;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;

public class SerializableScannerChunkTest {
    
    @Test
    public void testEncodeDecodeScannerChunk() throws IOException, ClassNotFoundException {
        SessionOptions options = new SessionOptions();
        options.setBatchTimeout(15, TimeUnit.MINUTES);
        
        Range range = new Range(new Key("row"), true, new Key("row\0"), false);
        Collection<Range> ranges = Collections.singleton(range);
        String server = "null server";
        
        ScannerChunk chunk = new ScannerChunk(options, ranges, server);
        
        SerializableScannerChunk serialized = SerializableScannerChunk.encode(chunk);
        byte[] bytes = SerializableScannerChunk.toBytes(serialized);
        
        SerializableScannerChunk decoded = (SerializableScannerChunk) SerializableScannerChunk.fromBytes(bytes);
        ScannerChunk decodedChunk = decoded.getScannerChunk();
        
        assertEquals(chunk.getQueryId(), decodedChunk.getQueryId());
        assertEquals(chunk.getOptions().getIterators(), decodedChunk.getOptions().getIterators());
        assertEquals(chunk.getRanges().iterator().next(), decodedChunk.getRanges().iterator().next());
    }
    
    @Test
    public void testEncodeDecodeRange() throws IOException {
        Range range = new Range(new Key("row"), true, new Key("row\0"), false);
        String encoded = SerializableScannerChunk.encodeRange(range);
        Range decoded = SerializableScannerChunk.decodeRange(encoded);
        assertEquals(range, decoded);
    }
}
