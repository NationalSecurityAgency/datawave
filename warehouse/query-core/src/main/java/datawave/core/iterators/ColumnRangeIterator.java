package datawave.core.iterators;

import org.apache.accumulo.core.data.ByteSequence;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.IteratorEnvironment;
import org.apache.accumulo.core.iterators.SkippingIterator;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iteratorsImpl.system.InterruptibleIterator;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.io.Text;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * This class and its sub classes implement Range queries against Column Families
 * 
 */
public abstract class ColumnRangeIterator extends SkippingIterator implements InterruptibleIterator {
    
    public static final String RANGE_NAME = "RANGE_NAME";
    public static final String SKIP_LIMIT_NAME = "SKIP_LIMIT";
    public static final String SCAN_LIMIT_NAME = "SCAN_LIMIT";
    
    protected Range columnRange = null;
    protected Range range = null;
    private Collection<ByteSequence> columnFamilies;
    private boolean inclusive;
    protected int skipLimit = 10;
    protected long scanLimit = Long.MAX_VALUE;
    protected long scanCount = 0;
    
    public ColumnRangeIterator() {
        super();
    }
    
    public ColumnRangeIterator(SortedKeyValueIterator<Key,Value> source) {
        this.setSource(source);
    }
    
    public ColumnRangeIterator(SortedKeyValueIterator<Key,Value> source, Range columnRange) {
        this.setSource(source);
        this.columnRange = columnRange;
    }
    
    @Override
    public void init(SortedKeyValueIterator<Key,Value> source, Map<String,String> options, IteratorEnvironment env) throws IOException {
        
        super.init(source, options, env);
        
        String e = options.get(RANGE_NAME);
        if (e == null)
            throw new IOException("COLUMN_FAMILY_RANGE_NAME must be set");
        columnRange = decodeRange(e);
        
        String num = options.get(SKIP_LIMIT_NAME);
        if (num != null) {
            skipLimit = Integer.parseInt(num);
        } else {
            skipLimit = 10;
        }
        
        num = options.get(SCAN_LIMIT_NAME);
        if (num != null) {
            scanLimit = Long.parseLong(num);
        } else {
            scanLimit = Long.MAX_VALUE;
        }
    }
    
    protected void setColumnRange(Range columnRange) {
        this.columnRange = columnRange;
    }
    
    public Range getColumnRange() {
        return columnRange;
    }
    
    protected int getSkipLimit() {
        return skipLimit;
    }
    
    protected void setSkipLimit(int skipLimit) {
        this.skipLimit = skipLimit;
    }
    
    public long getScanLimit() {
        return scanLimit;
    }
    
    public void setScanLimit(long scanLimit) {
        this.scanLimit = scanLimit;
    }
    
    public static String encodeRange(Range range) throws IOException {
        ByteArrayOutputStream b = new ByteArrayOutputStream();
        DataOutputStream d = new DataOutputStream(b);
        
        try {
            range.write(d);
        } catch (Exception e) {
            throw new IOException(e);
        } finally {
            d.close();
            b.close();
        }
        
        return new String(Base64.encodeBase64(b.toByteArray()));
    }
    
    public static Range decodeRange(String e) throws IOException {
        ByteArrayInputStream b = new ByteArrayInputStream(Base64.decodeBase64(e.getBytes()));
        DataInputStream in = new DataInputStream(b);
        Range range = new Range();
        try {
            range.readFields(in);
        } catch (Exception e2) {
            throw new IOException(e2);
        } finally {
            in.close();
            b.close();
        }
        
        return range;
    }
    
    protected byte[] followingArray(byte ba[]) {
        byte[] fba = new byte[ba.length + 1];
        System.arraycopy(ba, 0, fba, 0, ba.length);
        fba[ba.length] = (byte) 0x00;
        return fba;
    }
    
    @Override
    protected final void consume() throws IOException {
        // execute the consume implementation
        consumeImpl();
        // now reset the scan count back to 0 for the next round.
        scanCount = 0;
    }
    
    protected abstract void consumeImpl() throws IOException;
    
    protected void advanceSource() throws IOException {
        if (scanCount >= scanLimit) {
            throw new ScanLimitReached(getSource().getTopKey(), "Reached scan limit of " + scanLimit);
        }
        ++scanCount;
        getSource().next();
    }
    
    protected void reseek(Key key) throws IOException {
        if (scanCount >= scanLimit) {
            throw new ScanLimitReached(getSource().getTopKey(), "Reached scan limit of " + scanLimit);
        }
        ++scanCount;
        
        if (range.afterEndKey(key)) {
            range = new Range(range.getEndKey(), true, range.getEndKey(), range.isEndKeyInclusive());
        } else {
            range = new Range(key, true, range.getEndKey(), range.isEndKeyInclusive());
        }
        getSource().seek(range, columnFamilies, inclusive);
    }
    
    @Override
    public void seek(Range range, Collection<ByteSequence> columnFamilies, boolean inclusive) throws IOException {
        
        this.range = range;
        this.columnFamilies = columnFamilies;
        this.inclusive = inclusive;
        super.seek(range, columnFamilies, inclusive);
    }
    
    @Override
    public void setInterruptFlag(AtomicBoolean flag) {
        ((InterruptibleIterator) getSource()).setInterruptFlag(flag);
    }
    
    protected Text getColumnStart() {
        Key startKey = getColumnRange().getStartKey();
        if (startKey == null)
            return new Text("");
        else
            return getColumnRange().getStartKey().getRow();
    }
    
    public static class ScanLimitReached extends RuntimeException {
        private final Key lastKey;
        
        public ScanLimitReached(Key key, String message) {
            super(message);
            lastKey = key;
        }
        
        public Key getLastKey() {
            return lastKey;
        }
    }
}
