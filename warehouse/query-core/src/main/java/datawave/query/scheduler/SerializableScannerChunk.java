package datawave.query.scheduler;

import datawave.query.config.ShardQueryConfiguration;
import datawave.query.tables.SessionOptions;
import datawave.query.tables.async.ScannerChunk;
import org.apache.accumulo.core.data.Column;
import org.apache.accumulo.core.data.Range;
import org.apache.hadoop.io.Text;
import org.apache.kerby.util.Base64;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class SerializableScannerChunk implements Serializable {
    
    ShardQueryConfiguration config;
    List<byte[]> fetchedColumns;
    Collection<String> ranges;
    String lastKnownLocation;
    String queryId;
    
    /**
     * Extracts all relevant info from a ScannerChunk into this class
     *
     * @param chunk
     * @return
     */
    public static SerializableScannerChunk encode(ScannerChunk chunk) {
        SerializableScannerChunk ser = new SerializableScannerChunk();
        
        // extract all session options
        SessionOptions options = chunk.getOptions();
        ser.config = options.getConfiguration();
        
        // handle fetched column families
        ser.fetchedColumns = new ArrayList<>();
        for (Column column : options.getFetchedColumns()) {
            ser.fetchedColumns.add(column.columnFamily);
        }
        
        // encode ranges
        ser.ranges = new ArrayList<>();
        for (Range range : chunk.getRanges()) {
            try {
                ser.ranges.add(encodeRange(range));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        // set remaining variables
        ser.lastKnownLocation = chunk.getLastKnownLocation();
        ser.queryId = chunk.getQueryId();
        
        return ser;
    }
    
    /**
     * Build a ScannerChunk from serialized data
     *
     * @return a ScannerChunk
     */
    public ScannerChunk getScannerChunk() {
        
        SessionOptions options = new SessionOptions();
        options.setQueryConfig(config);
        for (byte[] bytes : fetchedColumns) {
            options.fetchColumnFamily(new Text(bytes));
        }
        
        Collection<Range> decodedRanges = new ArrayList<>();
        for (String s : ranges) {
            try {
                decodedRanges.add(decodeRange(s));
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        
        String server = lastKnownLocation;
        
        ScannerChunk chunk = new ScannerChunk(options, decodedRanges, server);
        chunk.setQueryId(queryId);
        return chunk;
    }
    
    /**
     * Encode a range into a string of bytes
     *
     * @param range
     *            a range
     * @return a string of bytes
     * @throws IOException
     */
    public static String encodeRange(Range range) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        try {
            range.write(d);
        } finally {
            d.close();
            b.close();
        }
        return new String(Base64.encodeBase64(b.toByteArray()), StandardCharsets.UTF_8);
    }
    
    /**
     * Decode a byte string into a Range
     * 
     * @param s
     *            byte string
     * @return a Range
     * @throws IOException
     */
    public static Range decodeRange(String s) throws IOException {
        ByteArrayInputStream b = new ByteArrayInputStream(Base64.decodeBase64(s.getBytes(StandardCharsets.UTF_8)));
        DataInputStream d = new DataInputStream(b);
        Range range = new Range();
        try {
            range.readFields(d);
        } finally {
            d.close();
            b.close();
        }
        return range;
    }
    
    public static byte[] toBytes(Serializable o) {
        // now serialize to bytes
        byte[] bytes = null;
        try {
            ByteArrayOutputStream boas = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(boas);
            oos.writeObject(o);
            bytes = boas.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return bytes;
    }
    
    public static Object fromBytes(byte[] data) throws IOException, ClassNotFoundException {
        ObjectInputStream ois = new ObjectInputStream(new ByteArrayInputStream(data));
        return ois.readObject();
    }
}
